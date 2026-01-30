use crate::error::{Error, Result};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    pub postgres_url: Option<String>,
    pub log_level: Option<String>,
    pub cursor_timeout_secs: Option<u64>,
    pub cursor_sweep_interval_secs: Option<u64>,
    #[serde(default)]
    pub shadow: Option<ShadowConfig>,
    // Server TLS configuration
    #[serde(default)]
    pub tls_cert_file: Option<String>,
    #[serde(default)]
    pub tls_key_file: Option<String>,
    #[serde(default)]
    pub tls_ca_file: Option<String>,
    #[serde(default)]
    pub tls_client_auth: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Default to Mongo's standard port locally
            listen_addr: "127.0.0.1:27017".to_string(),
            postgres_url: None,
            log_level: None,
            cursor_timeout_secs: Some(300),
            cursor_sweep_interval_secs: Some(30),
            shadow: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_file: None,
            tls_client_auth: false,
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
    #[allow(clippy::too_many_arguments)]
    pub fn with_overrides(
        mut self,
        listen_addr: Option<String>,
        postgres_url: Option<String>,
        log_level: Option<String>,
        shadow_enabled: Option<bool>,
        shadow_addr: Option<String>,
        shadow_db_prefix: Option<String>,
        shadow_timeout_ms: Option<u64>,
        shadow_sample_rate: Option<f64>,
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
        if shadow_enabled.is_some()
            || shadow_addr.is_some()
            || shadow_db_prefix.is_some()
            || shadow_timeout_ms.is_some()
            || shadow_sample_rate.is_some()
        {
            let mut sh = self.shadow.unwrap_or_default();
            if let Some(v) = shadow_enabled {
                sh.enabled = v;
            }
            if let Some(v) = shadow_addr {
                sh.addr = v;
            }
            if let Some(v) = shadow_db_prefix {
                sh.db_prefix = Some(v);
            }
            if let Some(v) = shadow_timeout_ms {
                sh.timeout_ms = v;
            }
            if let Some(v) = shadow_sample_rate {
                sh.sample_rate = v;
            }
            self.shadow = Some(sh);
        }
        self
    }

    /// Validate the configuration
    #[allow(clippy::collapsible_if)]
    pub fn validate(&self) -> Result<()> {
        // Validate listen_addr
        if self.listen_addr.is_empty() {
            return Err(Error::Msg("listen_addr cannot be empty".to_string()));
        }
        if !self.listen_addr.contains(':') {
            return Err(Error::Msg(format!(
                "listen_addr '{}' must be in host:port format",
                self.listen_addr
            )));
        }

        // Validate postgres_url if provided
        if let Some(ref url) = self.postgres_url {
            if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
                return Err(Error::Msg(format!(
                    "postgres_url '{}' must start with postgres:// or postgresql://",
                    url
                )));
            }
        }

        // Validate shadow config if enabled
        if let Some(ref shadow) = self.shadow {
            if shadow.enabled {
                if shadow.addr.is_empty() {
                    return Err(Error::Msg(
                        "shadow.addr cannot be empty when shadow is enabled".to_string(),
                    ));
                }
                if !shadow.addr.contains(':') {
                    return Err(Error::Msg(format!(
                        "shadow.addr '{}' must be in host:port format",
                        shadow.addr
                    )));
                }
                if shadow.sample_rate < 0.0 || shadow.sample_rate > 1.0 {
                    return Err(Error::Msg(format!(
                        "shadow.sample_rate must be between 0.0 and 1.0, got {}",
                        shadow.sample_rate
                    )));
                }

                // Validate TLS config if enabled
                if shadow.tls_enabled {
                    if let Some(ref ca_file) = shadow.tls_ca_file {
                        if !std::path::Path::new(ca_file).exists() {
                            return Err(Error::Msg(format!(
                                "shadow.tls_ca_file '{}' does not exist",
                                ca_file
                            )));
                        }
                    }
                    if let Some(ref cert_file) = shadow.tls_client_cert {
                        if !std::path::Path::new(cert_file).exists() {
                            return Err(Error::Msg(format!(
                                "shadow.tls_client_cert '{}' does not exist",
                                cert_file
                            )));
                        }
                    }
                    if let Some(ref key_file) = shadow.tls_client_key {
                        if !std::path::Path::new(key_file).exists() {
                            return Err(Error::Msg(format!(
                                "shadow.tls_client_key '{}' does not exist",
                                key_file
                            )));
                        }
                    }
                }
            }
        }

        // Validate server TLS configuration
        if self.tls_cert_file.is_some() || self.tls_key_file.is_some() {
            // If one is set, both must be set
            if self.tls_cert_file.is_none() {
                return Err(Error::Msg(
                    "tls_cert_file must be set when tls_key_file is set".to_string(),
                ));
            }
            if self.tls_key_file.is_none() {
                return Err(Error::Msg(
                    "tls_key_file must be set when tls_cert_file is set".to_string(),
                ));
            }

            // Validate files exist
            if let Some(ref cert_file) = self.tls_cert_file {
                if !std::path::Path::new(cert_file).exists() {
                    return Err(Error::Msg(format!(
                        "tls_cert_file '{}' does not exist",
                        cert_file
                    )));
                }
            }
            if let Some(ref key_file) = self.tls_key_file {
                if !std::path::Path::new(key_file).exists() {
                    return Err(Error::Msg(format!(
                        "tls_key_file '{}' does not exist",
                        key_file
                    )));
                }
            }
            if let Some(ref ca_file) = self.tls_ca_file {
                if !std::path::Path::new(ca_file).exists() {
                    return Err(Error::Msg(format!(
                        "tls_ca_file '{}' does not exist",
                        ca_file
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Deterministic sampling using request_id and db namespace hash
pub fn should_sample_deterministically(request_id: i32, db: &str, sample_rate: f64) -> bool {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Hash the combination of request_id and db
    let mut hasher = DefaultHasher::new();
    request_id.hash(&mut hasher);
    db.hash(&mut hasher);
    let hash = hasher.finish();

    // Convert hash to a value in [0, 1)
    let normalized = (hash as f64) / (u64::MAX as f64);

    // Sample if normalized value is less than sample_rate
    normalized < sample_rate
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowMode {
    CompareOnly,
    CompareAndFail,
    RecordOnly,
}

impl Default for ShadowMode {
    fn default() -> Self {
        ShadowMode::CompareOnly
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShadowCompareOptions {
    #[serde(default = "default_ignore_fields")]
    pub ignore_fields: Vec<String>,
    #[serde(default)]
    pub numeric_equivalence: bool,
}

fn default_ignore_fields() -> Vec<String> {
    vec![
        "$clusterTime".to_string(),
        "operationTime".to_string(),
        "topologyVersion".to_string(),
        "localTime".to_string(),
        "connectionId".to_string(),
    ]
}

impl Default for ShadowCompareOptions {
    fn default() -> Self {
        Self {
            ignore_fields: default_ignore_fields(),
            numeric_equivalence: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShadowConfig {
    #[serde(default)]
    pub enabled: bool,
    pub addr: String,
    #[serde(default)]
    pub db_prefix: Option<String>,
    #[serde(default = "default_shadow_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_shadow_sample_rate")]
    pub sample_rate: f64,
    #[serde(default)]
    pub mode: ShadowMode,
    #[serde(default)]
    pub compare: ShadowCompareOptions,
    #[serde(default)]
    pub deterministic_sampling: bool,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_shadow_auth_db")]
    pub auth_db: String,
    #[serde(default)]
    pub tls_enabled: bool,
    #[serde(default)]
    pub tls_ca_file: Option<String>,
    #[serde(default)]
    pub tls_client_cert: Option<String>,
    #[serde(default)]
    pub tls_client_key: Option<String>,
    #[serde(default)]
    pub tls_allow_invalid_certs: bool,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: "127.0.0.1:27018".to_string(),
            db_prefix: None,
            timeout_ms: default_shadow_timeout_ms(),
            sample_rate: default_shadow_sample_rate(),
            mode: ShadowMode::CompareOnly,
            compare: ShadowCompareOptions::default(),
            deterministic_sampling: false,
            username: None,
            password: None,
            auth_db: default_shadow_auth_db(),
            tls_enabled: false,
            tls_ca_file: None,
            tls_client_cert: None,
            tls_client_key: None,
            tls_allow_invalid_certs: false,
        }
    }
}

fn default_shadow_timeout_ms() -> u64 {
    800
}
fn default_shadow_sample_rate() -> f64 {
    1.0
}
fn default_shadow_auth_db() -> String {
    "admin".to_string()
}
