//बिजी७७<bandesh@gmail.com>

use reqwest::{Client, Url};
use scraper::{Html, Selector};
use rusqlite::{params, Connection, Result as SqliteResult};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio;
use futures::future::join_all;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::spawn_blocking;


const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let conn = Arc::new(Mutex::new(Connection::open("scraped_data.db")?));
    init_db(&conn)?;

    let firefox_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0";
    let client = Arc::new(Client::builder()
        .user_agent(firefox_user_agent.to_string())
        .build()?);

    let urls = vec![
        ("https://doc.rust-lang.org/book/", "Rust Book"),
        ("https://doc.rust-lang.org/rust-by-example/", "Rust by Example"),
    ];

   let tasks: Vec<_> = urls.into_iter().map(|(url, category)| {
        let client = Arc::clone(&client);
        let conn = Arc::clone(&conn);
        tokio::spawn(async move {
            scrape_url_with_resume(&client, &url, &conn, &category).await
        })
    }).collect();

    let results = join_all(tasks).await;
    for result in results {
        if let Err(e) = result {
            eprintln!("Task error: {}", e);
        }
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
                
                // Use spawn_blocking for CPU-intensive HTML parsing and convert to String
                let parsed_content = tokio::task::spawn_blocking(move || {
                    let document = Html::parse_document(&body);
                    let content_selector = Selector::parse("li.chapter, section.chapter, h1, h2, h3, h4, h5, h6, p, pre, code, ul, ol, li").unwrap();
                    document.select(&content_selector)
                        .map(|element| {
                            let html = element.html();
                            let element_type = element.value().name();
                            format!("{}|||{}", element_type, html)
                        })
                        .collect::<Vec<String>>()
                        .join("\n")
                }).await?;
                
                process_html(client, parsed_content, url, conn, category).await?;
                println!("Finished processing content for {}", url);
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

async fn process_html(
    client: &Client,
    parsed_content: String,
    url: &str,
    conn: &Arc<Mutex<Connection>>,
    category: &str
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Processing url: {}", url);
    let last_scraped_id = get_last_scraped_id(&conn, url)?;
    println!("Last scraped ID for {}: {}", url, last_scraped_id);

    let elements: Vec<_> = parsed_content.split('\n')
        .enumerate()
        .map(|(index, line)| {
            let parts: Vec<&str> = line.splitn(2, "|||").collect();
            if parts.len() == 2 {
                (index as i64 + 1, parts[1].to_string(), parts[0].to_string())
            } else {
                (index as i64 + 1, String::new(), String::new())
            }
        })
        .collect();

    for (current_id, html, element_type) in elements {
        if current_id <= last_scraped_id {
            continue;
        }

        let (topic, content, content_type) = process_element(client, &html, &element_type, url).await?;

        if !topic.is_empty() || !content.is_empty() {
            insert_data(conn, url, &topic, &content, current_id, category, &content_type)?;
            println!("Inserted item with ID: {} for URL: {}", current_id, url);
        }
    }

    println!("Finished processing HTML for {}", url);
    Ok(())
}

async fn process_element(
    client: &Client,
    html: &str,
    element_type: &str,
    base_url: &str
) -> Result<(String, String, String), Box<dyn Error + Send + Sync>> {
    // Clone the data that needs to be moved into the spawn_blocking closure
    let html = html.to_string();
    let element_type = element_type.to_string();

    // Use spawn_blocking for CPU-intensive HTML parsing
    let (topic, content, content_type) = spawn_blocking(move || {
        let fragment = Html::parse_fragment(&html);
        let element = fragment.root_element();

        match element_type.as_str() {
            "li" | "section" if element.value().attr("class").map_or(false, |c| c.contains("chapter")) => {
                let topic = element.text().collect::<String>().trim().to_string();
                if let Some(a_element) = element.select(&Selector::parse("a").unwrap()).next() {
                    if let Some(href) = a_element.value().attr("href") {
                        return (topic, href.to_string(), "linked_content".to_string());
                    }
                }
                (topic, String::new(), "chapter".to_string())
            },
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                let text = element.text().collect::<String>().trim().to_string();
                (text.clone(), text, "heading".to_string())
            },
            "p" => {
                let text = element.text().collect::<String>().trim().to_string();
                (String::new(), text, "paragraph".to_string())
            },
            "pre" | "code" => {
                let text = element.text().collect::<String>().trim().to_string();
                (String::new(), format!("<code>{}</code>", text), "code".to_string())
            },
            "ul" | "ol" => {
                let items: Vec<String> = element
                    .select(&Selector::parse("li").unwrap())
                    .map(|li| li.text().collect::<String>().trim().to_string())
                    .collect();
                let content = items.join("\n- ");
                (String::new(), format!("- {}", content), "list".to_string())
            },
            _ => (String::new(), String::new(), String::new()),
        }
    }).await?;

    // If it's linked content, fetch it asynchronously
    if content_type == "linked_content" {
        if let Ok(full_url) = Url::parse(base_url).and_then(|base| base.join(&content)) {
            let fetched_content = scrape_linked_content(client, &full_url.to_string()).await?;
            Ok((topic, fetched_content, content_type))
        } else {
            Ok((topic, content, content_type))
        }
    } else {
        Ok((topic, content, content_type))
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