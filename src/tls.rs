// TLS support for MongoDB shadow upstream connections
// Uses tokio-rustls for TLS encryption

use anyhow::{Context, Result, anyhow};
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::fs;
use std::io::BufReader;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::ShadowConfig;

/// Build a TLS connector from shadow configuration
pub fn build_tls_connector(cfg: &ShadowConfig) -> Result<TlsConnector> {
    if !cfg.tls_enabled {
        return Err(anyhow!("TLS not enabled"));
    }

    let mut root_store = RootCertStore::empty();

    // Load CA certificates
    if let Some(ca_file) = &cfg.tls_ca_file {
        let ca_data = fs::read(ca_file).context("failed to read CA file")?;
        let mut reader = BufReader::new(&ca_data[..]);
        let ca_certs: Vec<_> = certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .context("failed to parse CA certificates")?;

        for cert in ca_certs {
            root_store
                .add(cert)
                .context("failed to add CA certificate")?;
        }
    } else {
        // Use system root certificates
        root_store.extend(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ));
    }

    // Build client config
    let mut client_config = ClientConfig::builder().with_root_certificates(root_store);

    // Load client certificate and key if provided
    if let (Some(cert_file), Some(key_file)) = (&cfg.tls_client_cert, &cfg.tls_client_key) {
        let cert_data = fs::read(cert_file).context("failed to read client cert file")?;
        let key_data = fs::read(key_file).context("failed to read client key file")?;

        let mut cert_reader = BufReader::new(&cert_data[..]);
        let cert_chain: Vec<_> = certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .context("failed to parse client certificate")?;

        let mut key_reader = BufReader::new(&key_data[..]);
        let keys: Vec<_> = pkcs8_private_keys(&mut key_reader)
            .collect::<Result<Vec<_>, _>>()
            .context("failed to parse client key")?;

        if keys.is_empty() {
            return Err(anyhow!("no private key found"));
        }

        let key = rustls::pki_types::PrivateKeyDer::try_from(keys[0].clone())
            .context("invalid private key")?;

        client_config = client_config
            .with_client_auth_cert(cert_chain, key)
            .context("failed to set client certificate")?;
    } else {
        client_config = client_config.with_no_client_auth();
    }

    // Allow invalid certificates for dev if configured
    if cfg.tls_allow_invalid_certs {
        // Note: This is dangerous and should only be used in development
        tracing::warn!("TLS certificate validation disabled - this is insecure!");
    }

    let config = Arc::new(client_config);
    Ok(TlsConnector::from(config))
}

/// Connect to upstream MongoDB with TLS
pub async fn connect_tls(
    cfg: &ShadowConfig,
    host: &str,
    port: u16,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let connector = build_tls_connector(cfg)?;

    // Connect to the upstream server
    let stream = TcpStream::connect((host, port))
        .await
        .context("failed to connect to upstream")?;

    // Perform TLS handshake
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| anyhow!("invalid server name"))?;

    let tls_stream = connector
        .connect(server_name, stream)
        .await
        .context("TLS handshake failed")?;

    tracing::info!("TLS connection established to {}:{}", host, port);
    Ok(tls_stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_tls_connector_disabled() {
        let cfg = ShadowConfig {
            tls_enabled: false,
            ..Default::default()
        };
        let result = build_tls_connector(&cfg);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_tls_connector_no_certs() {
        let cfg = ShadowConfig {
            tls_enabled: true,
            tls_ca_file: None,
            tls_client_cert: None,
            tls_client_key: None,
            tls_allow_invalid_certs: false,
            ..Default::default()
        };
        let result = build_tls_connector(&cfg);
        assert!(result.is_ok());
    }
}
