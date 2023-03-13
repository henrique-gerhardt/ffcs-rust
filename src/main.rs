use axum::{
    error_handling::HandleErrorLayer, http::StatusCode, routing::post, BoxError, Extension, Json,
    Router, Server,
};
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::Duration,
};
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{Schema, STORED, TEXT},
    Index, IndexWriter, Result,
};
use tokio::sync::Mutex;
use tower::ServiceBuilder;

#[derive(Serialize, Deserialize)]
struct SearchRequest {
    path: String,
    query: String,
}

#[derive(Serialize, Deserialize)]
struct SearchResult {
    file_path: String,
    line_number: usize,
    line_content: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Mouting Index...");
    let index_dir = Path::new("./index");
    let schema = create_schema();
    let index = match index_dir.join("meta.json").exists() {
        true => Index::open_in_dir(index_dir)?,
        false => Index::create_in_dir(index_dir, schema.clone())?,
    };
    let writer = index.writer(50_000_000)?;
    let writer = Arc::new(Mutex::new(writer));
    let dir_map = Arc::new(Mutex::new(build_dir_map(index_dir).await));

    let server_task = server_task(index);
    let watcher_task = watcher_task(index_dir, writer.clone(), dir_map.clone());

    tokio::select! {
        _ = server_task => {},
        _ = watcher_task => {},
    }

    Ok(())
}

async fn server_task(index: Index) -> Result<()> {
    println!("Starting Server...");
    let middleware = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_timeout_error))
        .timeout(Duration::from_secs(30));

    let app = Router::new()
        .layer(Extension(index))
        .layer(middleware)
        .route("/search", post(search));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    println!("Listening on {}", addr);

    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn handle_timeout_error(err: BoxError) -> (StatusCode, String) {
    if err.is::<tower::timeout::error::Elapsed>() {
        (
            StatusCode::REQUEST_TIMEOUT,
            "Request took too long".to_string(),
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unhandled internal error: {}", err),
        )
    }
}

async fn watcher_task(
    index_dir: &Path,
    writer: Arc<Mutex<IndexWriter>>,
    dir_map: Arc<Mutex<HashMap<String, Vec<String>>>>,
) -> Result<()> {
    println!("Starting Watcher...");
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                tx.send(res).await.unwrap_or_default();
            })
        },
        Config::default(),
    )
    .unwrap();

    watcher.watch(index_dir, RecursiveMode::Recursive).unwrap();

    loop {
        match rx.recv().await {
            Some(event) => match event {
                Ok(event) => match event.kind {
                    EventKind::Modify(_) | EventKind::Create(_) => {
                        let path = event.paths[0].clone();
                        if let Some(ext) = path.extension() {
                            if ext == "txt" {
                                let mut dir_map = dir_map.lock().await;
                                let file_name = path.file_name().unwrap().to_str().unwrap();
                                let dir_path = path.parent().unwrap();
                                let dir_key = dir_path.to_str().unwrap().to_owned();
                                let file_paths = dir_map.get(&dir_key).unwrap();
                                if !file_paths.contains(&file_name.to_owned()) {
                                    let file_paths = dir_map.get_mut(&dir_key).unwrap();
                                    file_paths.push(file_name.to_owned());
                                    let writer = writer.clone();
                                    let schema = schema.clone();
                                    tokio::spawn(async move {
                                        let mut writer = writer.lock().await;
                                        index_file((&schema)(), &mut writer, &path).unwrap();
                                        writer.commit().unwrap();
                                    });
                                }
                            }
                        }
                    }
                    _ => {}
                },
                Err(e) => println!("watch error: {:?}", e),
            },
            None => break,
        }
    }

    Ok(())
}

fn create_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("file_path", STORED);
    schema_builder.add_text_field("line_content", TEXT);
    schema_builder.add_i64_field("line_number", STORED);
    schema_builder.build()
}

fn index_file(schema: &Schema, writer: &mut IndexWriter, path: &Path) -> Result<()> {
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut doc = schema.parse_document(&format!(
        r#"
        file_path: "{}"
        "#,
        path.to_str().unwrap()
    ))?;
    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        doc.add_text(schema.get_field("line_content").unwrap(), &line);
        doc.add_i64(schema.get_field("line_number").unwrap(), line_number as i64);
        writer.add_document(doc.clone()).unwrap();
    }

    Ok(())
}

async fn search(
    index: Extension<Index>,
    request: axum::extract::Json<SearchRequest>,
) -> Json<Vec<SearchResult>> {
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let query_parser =
        QueryParser::for_index(&index, vec![schema().get_field("line_content").unwrap()]);
    let query = query_parser.parse_query(&request.query).unwrap();

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
    let mut results = vec![];
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address).unwrap();
        let file_path: String = retrieved_doc
            .get_first(schema().get_field("file_path").unwrap())
            .unwrap()
            .as_text()
            .unwrap()
            .to_string();
        let line_number = retrieved_doc
            .get_first(schema().get_field("line_number").unwrap())
            .unwrap()
            .as_i64()
            .unwrap() as usize;
        let line_content = retrieved_doc
            .get_first(schema().get_field("line_content").unwrap())
            .unwrap()
            .as_text()
            .unwrap()
            .to_string();
        let result = SearchResult {
            file_path,
            line_number,
            line_content,
        };
        results.push(result);
    }

    Json(results)
}

fn schema() -> &'static Schema {
    lazy_static::lazy_static! {
        static ref SCHEMA: Schema = create_schema();
    }

    &SCHEMA
}

async fn build_dir_map(index_dir: &Path) -> std::collections::HashMap<String, Vec<String>> {
    let mut map = std::collections::HashMap::new();
    let dir = std::fs::read_dir(index_dir).unwrap();
    for entry in dir {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "txt" {
                        let file_name = path.file_name().unwrap().to_str().unwrap().to_owned();
                        let dir_path = path.parent().unwrap();
                        let dir_key = dir_path.to_str().unwrap().to_owned();
                        let file_paths = map.entry(dir_key).or_insert(vec![]);
                        file_paths.push(file_name);
                    }
                }
            }
        }
    }
    map
}
