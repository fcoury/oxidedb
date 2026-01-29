use rand::{Rng, distributions::Alphanumeric};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::NoTls;

pub struct TestDb {
    admin_url: String,
    pub dbname: String,
    pub url: String,
}

impl TestDb {
    pub async fn provision_from_env() -> Option<Self> {
        let admin_url = match std::env::var("OXIDEDB_TEST_POSTGRES_URL") {
            Ok(u) => u,
            Err(_) => return None,
        };
        let dbname = format!(
            "oxidedb_bench_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            rand_suffix(6)
        );
        let url = replace_db_name(&admin_url, &dbname);

        // Create the database
        let (client, conn) = tokio_postgres::connect(&admin_url, NoTls).await.ok()?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let qname = q_ident(&dbname);
        client
            .batch_execute(&format!("CREATE DATABASE {} TEMPLATE template0", qname))
            .await
            .ok()?;
        drop(client);

        Some(Self {
            admin_url,
            dbname,
            url,
        })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Ensure drop even if test panics. Use a dedicated thread + runtime so
        // we do not depend on any ambient tokio runtime still being alive.
        let admin_url = self.admin_url.clone();
        let dbname = self.dbname.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                rt.block_on(async move {
                    if let Ok((client, conn)) = tokio_postgres::connect(&admin_url, NoTls).await {
                        // Drive connection on this small runtime
                        tokio::spawn(async move { let _ = conn.await; });
                        let _ = client
                            .execute(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                                &[&dbname],
                            )
                            .await;
                        let qname = q_ident(&dbname);
                        let _ = client
                            .batch_execute(&format!("DROP DATABASE IF EXISTS {}", qname))
                            .await;
                    }
                });
            }
        })
        .join();
    }
}

fn rand_suffix(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

fn q_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn replace_db_name(url: &str, new_db: &str) -> String {
    // naive parsing: split before '?', then replace segment after last '/'
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let pos = base.rfind('/').unwrap_or(base.len());
    let prefix = &base[..pos + 1];
    let mut out = String::from(prefix);
    out.push_str(new_db);
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}
