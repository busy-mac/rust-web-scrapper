//बिजी७७<bandesh@gmail.com>

use reqwest::{Client, Url};
use scraper::{Html, Selector, ElementRef};
use scraper::Element;
use rusqlite::{params, Connection, Result as SqliteResult};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio;

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
        ("https://doc.rust-lang.org/book/", "Rust Book"),
        ("https://doc.rust-lang.org/rust-by-example/", "Rust by Example"),
    ];

    for (url, category) in urls {
        scrape_url_with_resume(&client, &url, &conn, category).await?;
    }

    Ok(())
}

async fn scrape_url_with_resume(
    client: &Client,
    url: &str,
    conn: &Arc<Mutex<Connection>>,
    category: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut retry_count = 0;

    loop {
        match client.get(url).send().await {
            Ok(response) => {
                println!("Successfully fetched URL: {}", url);
                let body = response.text().await?;
                println!("Received body of length: {}", body.len());
                process_html(client, &body, url, conn, category).await?;
                println!("Finished processing content");
                return Ok(());
            },
            Err(e) => {
                if retry_count >= MAX_RETRIES {
                    return Err(Box::new(e));
                }
                eprintln!("Error scraping {}: {}. Retrying in {} seconds...", url, e, RETRY_DELAY.as_secs());
                tokio::time::sleep(RETRY_DELAY).await;
                retry_count += 1;
            }
        }
    }
}

async fn process_html(client: &Client, body: &str, url: &str, conn: &Arc<Mutex<Connection>>, category: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Processing url: {}", url);
    let document = Html::parse_document(body);
    let last_scraped_id = get_last_scraped_id(&conn, url)?;
    println!("Last scraped ID: {}", last_scraped_id);

    let content_selector = Selector::parse("li.chapter, section.chapter, h1, h2, h3, h4, h5, h6, p, pre, code, ul, ol, li").unwrap();

    for (index, element) in document.select(&content_selector).enumerate() {
        let current_id = index as i64 + 1;  // Use 1-based indexing
        if current_id <= last_scraped_id {
            continue;
        }

        let (topic, content, content_type) = process_element(client, element, url).await?;

        if !topic.is_empty() || !content.is_empty() {
            insert_data(conn, url, &topic, &content, current_id, category, &content_type)?;
            println!("Inserted item with ID: {}", current_id);
        }
    }

    println!("Finished processing HTML");
    Ok(())
}

async fn process_element(client: &Client, element: ElementRef<'_>, base_url: &str) -> Result<(String, String, String), Box<dyn Error + Send + Sync>> {
    match element.value().name() {
        "li" | "section" if element.value().attr("class").map_or(false, |c| c.contains("chapter")) => {
            let topic = element.text().collect::<String>().trim().to_string();
            if let Some(a_element) = element.select(&Selector::parse("a").unwrap()).next() {
                if let Some(href) = a_element.value().attr("href") {
                    if let Ok(full_url) = Url::parse(base_url).and_then(|base| base.join(href)) {
                        let content = scrape_linked_content(client, &full_url.to_string()).await?;
                        return Ok((topic, content, "linked_content".to_string()));
                    }
                }
            }
            Ok((topic, String::new(), "chapter".to_string()))
        },
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
            let text = element.text().collect::<String>().trim().to_string();
            Ok((text.clone(), text, "heading".to_string()))
        },
        "p" => {
            let text = element.text().collect::<String>().trim().to_string();
            Ok((String::new(), text, "paragraph".to_string()))
        },
        "pre" | "code" => {
            let text = element.text().collect::<String>().trim().to_string();
            Ok((String::new(), format!("<code>{}</code>", text), "code".to_string()))
        },
        "ul" | "ol" => {
            let items: Vec<String> = element
                .select(&Selector::parse("li").unwrap())
                .map(|li| li.text().collect::<String>().trim().to_string())
                .collect();
            let content = items.join("\n- ");
            Ok((String::new(), format!("- {}", content), "list".to_string()))
        },
        _ => Ok((String::new(), String::new(), String::new())),
    }
}



async fn scrape_linked_content(client: &Client, url: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let response = client.get(url).send().await?;
    let body = response.text().await?;
    let document = Html::parse_document(&body);
    let content_selector = Selector::parse("p, h1, h2, h3, h4, h5, h6, pre, code").unwrap();
    
    let mut content = String::new();
    for element in document.select(&content_selector) {
        match element.value().name() {
            "pre" | "code" => {
                content.push_str(&format!("<code>{}</code>", element.text().collect::<String>().trim()));
            },
            _ => {
                content.push_str(&element.text().collect::<String>().trim());
            }
        }
        content.push_str("\n\n");
    }
    
    Ok(content.trim().to_string())
}

fn init_db(conn: &Arc<Mutex<Connection>>) -> SqliteResult<()> {
    let conn = conn.lock().unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scraped_data (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            topic TEXT NOT NULL,
            content TEXT NOT NULL,
            paragraph_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            content_type TEXT NOT NULL
        )",
        [],
    )?;
    Ok(())
}

fn insert_data(conn: &Arc<Mutex<Connection>>, url: &str, topic: &str, content: &str, paragraph_id: i64, category: &str, content_type: &str) -> SqliteResult<()> {
    let conn = conn.lock().unwrap();
    conn.execute(
        "INSERT INTO scraped_data (url, topic, content, paragraph_id, category, content_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![url, topic, content, paragraph_id, category, content_type],
    )?;
    
    println!("Inserted data with ID: {} for URL: {}", paragraph_id, url);
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