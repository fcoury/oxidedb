use clap::Parser;
use oxidedb::{config::Config, server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env first so clap's env fallbacks see variables
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    // Attempt to load config before initializing logs to pick up log_level from file
    let cfg_file_res = Config::load_from_file(cli.config.as_deref());
    let cfg_file = match &cfg_file_res {
        Ok(c) => c.clone(),
        Err(_) => Config::default(),
    };

    // Determine log filter precedence: CLI (--log-level / OXIDEDB_LOG_LEVEL)
    // > RUST_LOG (env) > config.toml log_level > default("info")
    let filter_spec = if let Some(ref lvl) = cli.log_level {
        lvl.clone()
    } else if let Ok(env_spec) = std::env::var("RUST_LOG") {
        env_spec
    } else if let Some(ref lvl) = cfg_file.log_level {
        lvl.clone()
    } else {
        "info".to_string()
    };

    // Initialize logging with chosen filter
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter_spec))
        .compact()
        .init();

    if let Err(e) = cfg_file_res.as_ref() {
        tracing::warn!(error = %format!("{e:?}"), "invalid config; using defaults");
    }

    // Load from config file (if present), then override with CLI/env.
    let cfg = cfg_file.with_overrides(
        cli.listen_addr.clone(),
        cli.postgres_url.clone(),
        cli.log_level.clone(),
    );
    tracing::info!(listen_addr = %cfg.listen_addr, "starting oxidedb");

    if let Err(e) = server::run(cfg).await {
        tracing::error!(error = %format!("{e:?}"), "server terminated with error");
    }

    Ok(())
}

#[derive(Debug, Parser, Clone)]
#[command(name = "oxidedb", version, about = "Mongo wire server on Postgres jsonb")]
struct Cli {
    /// Path to config TOML file
    #[arg(short = 'c', long = "config", env = "OXIDEDB_CONFIG")]
    config: Option<String>,

    /// Listen address for the server (e.g., 127.0.0.1:27017)
    #[arg(long = "listen-addr", env = "OXIDEDB_LISTEN_ADDR")]
    listen_addr: Option<String>,

    /// PostgreSQL connection URL
    #[arg(long = "postgres-url", env = "OXIDEDB_POSTGRES_URL")]
    postgres_url: Option<String>,

    /// Log level or filter spec (e.g., info or info,oxidedb=debug)
    #[arg(long = "log-level", env = "OXIDEDB_LOG_LEVEL")]
    log_level: Option<String>,
}
