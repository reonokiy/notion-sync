use log::{info, warn};
use tokio::time::{interval, Duration};

use crate::{sync, AppState};

pub fn spawn_periodic_sync(state: AppState, interval_seconds: u64) {
    let interval_seconds = interval_seconds.max(1);
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_seconds));
        loop {
            ticker.tick().await;
            info!("starting scheduled sync");
            if let Err(err) = sync::sync_all(&state).await {
                warn!("scheduled sync failed: {err}");
            }
        }
    });
}
