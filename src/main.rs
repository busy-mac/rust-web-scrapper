//बिजी७७<bandesh@gmail.com>

use reqwest::{Client, Url};
use scraper::{Html, Selector};
use rusqlite::{params, Connection, Result as SqliteResult};
use std::collections::HashSet;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio;
use std::future::Future;
use std::pin::Pin;

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let conn = Arc::new(Mutex::new(Connection::open("scraped_data.db")?));
    init_db(&conn)?;

    let firefox_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0";

    let client = Client::builder()
        .user_agent(firefox_user_agent.to_string())
        .build()?;

    let urls = vec![
        "https://doc.rust-lang.org/book/",
        "https://doc.rust-lang.org/rust-by-example/",
    ];

    let handles: Vec<_> = urls.into_iter()
        .map(|url| {
            let client = client.clone();
            let conn = Arc::clone(&conn);
            tokio::spawn(async move {
                match scrape_url_with_resume(&client, &url, &conn).await {
                    Ok(_) => println!("Successfully scraped: {}", url),
                    Err(e) => eprintln!("Error scraping {}: {}", url, e),
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await?;
    }

    Ok(())
}

fn scrape_url_with_resume<'a>(
    client: &'a Client,
    url: &'a str,
    conn: &'a Arc<Mutex<Connection>>
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'a>> {
    Box::pin(async move {
        let mut retry_count = 0;

        loop {
            match scrape_url(client, url, Arc::clone(conn)).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retry_count >= MAX_RETRIES {
                        return Err(e);
                    }
                    eprintln!("Error scraping {}: {}. Retrying in {} seconds...", url, e, RETRY_DELAY.as_secs());
                    tokio::time::sleep(RETRY_DELAY).await;
                    retry_count += 1;
                }
            }
        }
    })
}

async fn scrape_url(client: &Client, url: &str, conn: Arc<Mutex<Connection>>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let response = client.get(url).send().await?;
    let body = response.text().await?;

    // Process the HTML in a separate function
    process_html(&body, url, &conn)?;

    // Extract and follow links one level deep
    let new_urls = extract_urls(&body, url)?;

    for new_url in new_urls {
        if !url_processed(&conn, &new_url)? {
            scrape_url_with_resume(client, &new_url, &conn).await?;
        }
    }

    Ok(())
}

fn process_html(body: &str, url: &str, conn: &Arc<Mutex<Connection>>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let document = Html::parse_document(body);
    let last_scraped_id = {
        let _conn_guard = conn.lock().unwrap();
        get_last_scraped_id(&conn, url)?
    };

    let paragraph_selector = Selector::parse("p").unwrap();
    let paragraphs_to_insert: Vec<(String, i64)> = document
        .select(&paragraph_selector)
        .enumerate()
        .filter(|(id, _)| *id as i64 > last_scraped_id)
        .map(|(id, element)| {
            let text = element.text().collect::<Vec<_>>().join(" ");
            (text, id as i64)
        })
        .filter(|(text, _)| text.len() > 70)  // Only keep paragraphs with more than 70 characters
        .collect();

    for (text, id) in paragraphs_to_insert {
        insert_data(conn, url, &text, id)?;
    }

    Ok(())
}

fn extract_urls(body: &str, base_url: &str) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
    let document = Html::parse_document(body);
    let link_selector = Selector::parse("a[href]").unwrap();

    let new_urls: HashSet<String> = document
        .select(&link_selector)
        .filter_map(|element| {
            element.value().attr("href").and_then(|href| {
                if href.starts_with("https") {
                    Some(href.to_string())
                } else {
                    Url::parse(base_url)
                        .and_then(|base| base.join(href))
                        .map(|full_url| full_url.to_string())
                        .ok()
                }
            })
        })
        .collect();

    Ok(new_urls)
}

fn init_db(conn: &Arc<Mutex<Connection>>) -> SqliteResult<()> {
    let conn = conn.lock().unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scraped_data (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            content TEXT NOT NULL,
            paragraph_id INTEGER NOT NULL
        )",
        [],
    )?;
    Ok(())
}

fn insert_data(conn: &Arc<Mutex<Connection>>, url: &str, content: &str, paragraph_id: i64) -> SqliteResult<()> {
    let conn = conn.lock().unwrap();
    conn.execute(
        "INSERT INTO scraped_data (url, content, paragraph_id) VALUES (?1, ?2, ?3)",
        params![url, content, paragraph_id],
    )?;
    Ok(())
}

fn get_last_scraped_id(conn: &Arc<Mutex<Connection>>, url: &str) -> SqliteResult<i64> {
    let conn = conn.lock().unwrap();
    conn.query_row(
        "SELECT COALESCE(MAX(paragraph_id), -1) FROM scraped_data WHERE url = ?1",
        params![url],
        |row| row.get(0),
    )
}

fn url_processed(conn: &Arc<Mutex<Connection>>, url: &str) -> SqliteResult<bool> {
    let conn = conn.lock().unwrap();
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM scraped_data WHERE url = ?1)",
        params![url],
        |row| row.get(0),
    )
}
