use oxidedb::{config::Config, server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging with env filter, e.g.: RUST_LOG=info,oxidedb=debug
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .compact()
        .init();

    let cfg = Config::load().unwrap_or_default();
    tracing::info!(listen_addr = %cfg.listen_addr, "starting oxidedb");

    if let Err(e) = server::run(cfg).await {
        tracing::error!(error = %format!("{e:?}"), "server terminated with error");
    }

    Ok(())
}
