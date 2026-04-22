use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

pub async fn fetch_all(client: &Client, urls: &[String]) -> Vec<(String, Result<String>)> {
    let mut tasks = Vec::with_capacity(urls.len());

    for url in urls {
        let url = url.clone();
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let result = fetch_with_retry(&client, &url).await;
            (url, result)
        }));
    }

    let mut results = Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await {
            Ok(item) => results.push(item),
            Err(error) => results.push((
                "<task>".to_string(),
                Err(anyhow!("subscription fetch task failed: {error}")),
            )),
        }
    }

    results
}

async fn fetch_with_retry(client: &Client, url: &str) -> Result<String> {
    let mut delay = Duration::from_secs(1);
    let max_attempts = 3usize;

    for attempt in 1..=max_attempts {
        match fetch_once(client, url).await {
            Ok(body) => {
                info!(url = %url, attempt, "subscription fetched");
                return Ok(body);
            }
            Err(error) => {
                if attempt == max_attempts {
                    return Err(error).with_context(|| {
                        format!("failed to fetch {url} after {max_attempts} attempts")
                    });
                }

                warn!(url = %url, attempt, error = %error, "subscription fetch attempt failed");
                sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
        }
    }

    Err(anyhow!("unreachable retry loop state for {url}"))
}

async fn fetch_once(client: &Client, url: &str) -> Result<String> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("request failed for {url}"))?;

    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("HTTP {status} returned for {url}"));
    }

    response
        .text()
        .await
        .with_context(|| format!("failed to read response body for {url}"))
}
