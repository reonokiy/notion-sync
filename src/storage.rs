use std::str::FromStr;

use anyhow::{anyhow, Result};
use opendal::{Operator, Scheme};

use crate::config::BackendConfig;

pub fn init_opendal(backend: &BackendConfig) -> Result<Operator> {
    let scheme = Scheme::from_str(&backend.r#type)
        .map_err(|_| anyhow!("unsupported OPENDAL type: {}", backend.r#type))?;
    let map = backend.settings_as_strings();
    let op = Operator::via_iter(scheme, map)?;
    Ok(op)
}
