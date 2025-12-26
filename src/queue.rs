use anyhow::Result;
use log::{error, info, warn};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{sleep, Duration};

use crate::config::QueueConfig;
use crate::{sync, AppState};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SyncJob {
    SyncPageById { page_id: String },
    SyncPage { database_id: String, page_id: String },
    ScanDataSource { database_id: String, data_source_id: String },
}

pub struct QueueHandle {
    kind: QueueKind,
}

enum QueueKind {
    Memory { tx: Sender<SyncJob> },
    Redis { client: redis::Client, key: String },
}

pub struct QueueWorker {
    kind: WorkerKind,
}

enum WorkerKind {
    Memory { rx: Receiver<SyncJob> },
    Redis { client: redis::Client, key: String },
}

pub fn init_queue(config: &QueueConfig) -> Result<(QueueHandle, QueueWorker)> {
    if let Some(url) = config
        .redis_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
    {
        let client = redis::Client::open(url)?;
        let key = format!("{}:sync-jobs", config.name);
        let handle = QueueHandle {
            kind: QueueKind::Redis {
                client: client.clone(),
                key: key.clone(),
            },
        };
        let worker = QueueWorker {
            kind: WorkerKind::Redis { client, key },
        };
        Ok((handle, worker))
    } else {
        let (tx, rx) = mpsc::channel(256);
        let handle = QueueHandle {
            kind: QueueKind::Memory { tx },
        };
        let worker = QueueWorker {
            kind: WorkerKind::Memory { rx },
        };
        Ok((handle, worker))
    }
}

impl Clone for QueueHandle {
    fn clone(&self) -> Self {
        match &self.kind {
            QueueKind::Memory { tx } => Self {
                kind: QueueKind::Memory { tx: tx.clone() },
            },
            QueueKind::Redis { client, key } => Self {
                kind: QueueKind::Redis {
                    client: client.clone(),
                    key: key.clone(),
                },
            },
        }
    }
}

impl QueueHandle {
    pub async fn enqueue(&self, job: SyncJob) -> Result<()> {
        let description = describe_job(&job);
        let result = match &self.kind {
            QueueKind::Memory { tx } => tx
                .send(job)
                .await
                .map_err(|_| anyhow::anyhow!("queue closed")),
            QueueKind::Redis { client, key } => {
                let payload = serde_json::to_string(&job)
                    .map_err(|err| anyhow::anyhow!("serialize job: {err}"))?;
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|err| anyhow::anyhow!("redis connect: {err}"))?;
                conn.rpush::<_, _, ()>(key, payload)
                    .await
                    .map_err(|err| anyhow::anyhow!("redis enqueue: {err}"))
            }
        };

        match result {
            Ok(()) => {
                info!("queued {}", description);
                Ok(())
            }
            Err(err) => {
                error!("failed to enqueue {}: {err}", description);
                Err(err)
            }
        }
    }
}

pub fn spawn_sync_worker(state: AppState, worker: QueueWorker, queue: QueueHandle) {
    tokio::spawn(async move {
        match worker.kind {
            WorkerKind::Memory { rx } => run_memory_worker(state, rx, queue).await,
            WorkerKind::Redis { client, key } => run_redis_worker(state, client, key, queue).await,
        }
    });
}

pub async fn enqueue_initial_scan(state: &AppState) {
    for database in &state.databases {
        for data_source in &database.data_sources {
            let _ = state
                .queue
                .enqueue(SyncJob::ScanDataSource {
                    database_id: database.id.clone(),
                    data_source_id: data_source.id.clone(),
                })
                .await;
        }
    }
}

async fn run_memory_worker(state: AppState, mut rx: Receiver<SyncJob>, queue: QueueHandle) {
    info!("sync worker started (memory)");
    while let Some(job) = rx.recv().await {
        handle_job(&state, &queue, job).await;
        sleep(Duration::from_millis(200)).await;
    }
    info!("sync worker stopped (memory)");
}

async fn run_redis_worker(state: AppState, client: redis::Client, key: String, queue: QueueHandle) {
    info!("sync worker started (redis)");
    loop {
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("redis connect failed: {err}; retrying");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        loop {
            let result: Result<Option<(String, String)>, redis::RedisError> =
                conn.blpop(&key, 0.0).await;
            let payload = match result {
                Ok(Some((_k, payload))) => payload,
                Ok(None) => continue,
                Err(err) => {
                    warn!("redis blpop failed: {err}; reconnecting");
                    break;
                }
            };

            let job: SyncJob = match serde_json::from_str(&payload) {
                Ok(job) => job,
                Err(err) => {
                    warn!("invalid job payload in redis queue: {err}");
                    continue;
                }
            };
            handle_job(&state, &queue, job).await;
            sleep(Duration::from_millis(200)).await;
        }
    }
}

async fn handle_job(state: &AppState, queue: &QueueHandle, job: SyncJob) {
    let description = describe_job(&job);
    info!("processing {}", description);
    if let Err(err) = process_job(state, queue, job).await {
        warn!("{} failed: {err}; requeueing", description);
    }
}

async fn process_job(state: &AppState, queue: &QueueHandle, job: SyncJob) -> Result<()> {
    match job {
        SyncJob::SyncPageById { page_id } => {
            if let Err(err) = sync::sync_page_by_id(state, &page_id).await {
                requeue_after(queue.clone(), SyncJob::SyncPageById { page_id }, Duration::from_secs(10));
                return Err(err);
            }
        }
        SyncJob::SyncPage {
            database_id,
            page_id,
        } => {
            let database = state.databases.iter().find(|db| db.id == database_id);
            let Some(database) = database else {
                warn!("database {} not configured, dropping page {}", database_id, page_id);
                return Ok(());
            };
            if let Err(err) = sync::sync_page(state, database, &page_id).await {
                requeue_after(
                    queue.clone(),
                    SyncJob::SyncPage {
                        database_id,
                        page_id,
                    },
                    Duration::from_secs(10),
                );
                return Err(err);
            }
        }
        SyncJob::ScanDataSource {
            database_id,
            data_source_id,
        } => {
            let database = state.databases.iter().find(|db| db.id == database_id);
            let Some(_database) = database else {
                warn!(
                    "database {} not configured, dropping data source {}",
                    database_id, data_source_id
                );
                return Ok(());
            };
            let page_ids = match state
                .notion
                .query_data_source_page_ids(&data_source_id)
                .await
            {
                Ok(page_ids) => page_ids,
                Err(err) => {
                    requeue_after(
                        queue.clone(),
                        SyncJob::ScanDataSource {
                            database_id,
                            data_source_id,
                        },
                        Duration::from_secs(10),
                    );
                    return Err(err);
                }
            };
            info!(
                "found {} pages for data source {} (db {})",
                page_ids.len(),
                data_source_id,
                database_id
            );
            for page_id in page_ids {
                let _ = queue
                    .enqueue(SyncJob::SyncPage {
                        database_id: database_id.clone(),
                        page_id,
                    })
                    .await;
            }
        }
    }
    Ok(())
}

fn requeue_after(queue: QueueHandle, job: SyncJob, delay: Duration) {
    let description = describe_job(&job);
    tokio::spawn(async move {
        sleep(delay).await;
        let _ = queue.enqueue(job).await;
        info!("requeued {}", description);
    });
}

fn describe_job(job: &SyncJob) -> String {
    match job {
        SyncJob::SyncPageById { page_id } => format!("page sync {}", page_id),
        SyncJob::SyncPage {
            database_id,
            page_id,
        } => format!("page sync {} (db {})", page_id, database_id),
        SyncJob::ScanDataSource {
            database_id,
            data_source_id,
        } => format!("data source scan {} (db {})", data_source_id, database_id),
    }
}
