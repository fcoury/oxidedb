use crate::error::{Error, Result};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    pub postgres_url: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Default to Mongo's standard port locally
            listen_addr: "127.0.0.1:27017".to_string(),
            postgres_url: None,
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        // Priority: explicit file (OXIDEDB_CONFIG) or ./config.toml → env → defaults
        let path = std::env::var("OXIDEDB_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
        if let Ok(contents) = fs::read_to_string(&path) {
            match toml::from_str::<Config>(&contents) {
                Ok(mut cfg) => {
                    // Allow env overrides on top of file values
                    if let Ok(addr) = std::env::var("OXIDEDB_LISTEN_ADDR") {
                        cfg.listen_addr = addr;
                    }
                    if let Ok(pg) = std::env::var("OXIDEDB_POSTGRES_URL") {
                        cfg.postgres_url = Some(pg);
                    }
                    return Ok(cfg);
                }
                Err(e) => return Err(Error::Msg(format!("Failed to parse {}: {}", path, e))),
            }
        }

        let mut cfg = Config::default();
        if let Ok(addr) = std::env::var("OXIDEDB_LISTEN_ADDR") {
            cfg.listen_addr = addr;
        }
        if let Ok(pg) = std::env::var("OXIDEDB_POSTGRES_URL") {
            cfg.postgres_url = Some(pg);
        }
        Ok(cfg)
    }
}

