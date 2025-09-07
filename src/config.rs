use crate::error::{Error, Result};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    pub postgres_url: Option<String>,
    pub log_level: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Default to Mongo's standard port locally
            listen_addr: "127.0.0.1:27017".to_string(),
            postgres_url: None,
            log_level: None,
        }
    }
}

impl Config {
    /// Load configuration from a TOML file. If `path_opt` is None or the file is
    /// missing/unreadable, returns defaults. Parsing errors are returned.
    pub fn load_from_file(path_opt: Option<&str>) -> Result<Self> {
        let path = path_opt.unwrap_or("config.toml");
        match fs::read_to_string(path) {
            Ok(contents) => match toml::from_str::<Config>(&contents) {
                Ok(cfg) => Ok(cfg),
                Err(e) => Err(Error::Msg(format!("Failed to parse {}: {}", path, e))),
            },
            Err(_e) => Ok(Self::default()),
        }
    }

    /// Apply CLI/env overrides (highest precedence) to an existing config.
    pub fn with_overrides(
        mut self,
        listen_addr: Option<String>,
        postgres_url: Option<String>,
        log_level: Option<String>,
    ) -> Self {
        if let Some(addr) = listen_addr {
            self.listen_addr = addr;
        }
        if let Some(pg) = postgres_url {
            self.postgres_url = Some(pg);
        }
        if let Some(ll) = log_level {
            self.log_level = Some(ll);
        }
        self
    }
}
