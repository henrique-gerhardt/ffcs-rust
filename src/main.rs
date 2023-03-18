use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    routing::{get, post},
    BoxError, Extension, Json, Router, Server,
};
use clap::Parser;
use notify::RecursiveMode;
use notify_debouncer_mini::{new_debouncer, DebouncedEvent};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use rust_search::SearchBuilder;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::{Path, PathBuf, Component},
    sync::Arc,
    sync::Mutex,
    time::Duration,
};
use tantivy::{
    collector::{Count, TopDocs},
    query::{Query, QueryParser},
    schema::{Schema, FAST, STORED, TEXT, FacetOptions, Facet},
    Document, Index, IndexWriter, Result, Term,
};
use tower::ServiceBuilder;

use futures::executor::block_on;

#[derive(Serialize, Deserialize)]
struct SearchRequest {
    path: String,
    query: String,
}

#[derive(Serialize, Deserialize)]
struct IndexRequest {
    path: String,
}

#[derive(Serialize, Deserialize)]
struct SearchResult {
    quantity: usize,
    matches: Vec<Match>,
}
#[derive(Serialize, Deserialize)]
struct Match {
    file_path: String,
    line_number: usize,
    line_content: String,
}

#[derive(Parser, Clone)]
#[command(author="Henrique Gerhardt", version="1.0", about="Um serviço de API de pesquisa indexada nos arquivos locais", long_about = None)]
struct Args {
    /// Porta para iniciar o Serviço
    #[arg(
        short = 'p',
        long = "port",
        value_name = "PORTA",
        default_value = "4000"
    )]
    port: u16,

    /// Extensão dos Arquivos a serem pesquisados
    #[arg(short = 'e', long = "file_extension", value_name = "EXTENSAO")]
    file_extension: String,

    /// Pasta root de acesso indexada
    #[arg(short = 'f', long = "root_path", value_name = "CAMINHO", default_value = ".")]
    root_path: Option<PathBuf>,

    /// Pasta root onde será armazenados os índices
    #[arg(short = 'i', long = "index_folder", value_name = "CAMINHO_INDEX", default_value = "./fileIndex")]
    index_folder: Option<PathBuf>,

    /// Força reindexar a pasta raiz inteira, mesmo que ela já tenha uma pasta de indice
    #[arg(short = 'r', long = "reload_index", action = clap::ArgAction::Count)]
    force_index: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let args = Arc::new(args);

    let index_dir: &Path = match args.root_path {
        Some(ref path) => path.as_path(),
        None => Path::new("."),
    };

    let file_extension: &str = match args.file_extension.starts_with(".") {
        true => {
            let mut chrs = args.file_extension.chars();
            chrs.next();
            chrs.as_str()
        }
        false => args.file_extension.as_str(),
    };

    println!("Mouting Schema...");

    let schema = Arc::new(create_schema());

    let index_path = match args.index_folder.clone() {
        Some(folder) => folder,
        None => index_dir.join("fileIndex")
    };

    let first_index_exist = index_path.exists();

    let index = match first_index_exist {
        true => Index::open_in_dir(index_path)?,
        false => {
            _ = fs::remove_dir_all(&index_path);
            fs::create_dir_all(&index_path).unwrap();
            Index::create_in_dir(index_path, schema.as_ref().clone())?
        }
    };
    let writer = index.writer(50_000_000)?;
    let writer = Arc::new(Mutex::new(writer));

    if !first_index_exist || args.force_index > 0 {
        index_full_dir(index_dir, file_extension, writer.clone(), schema.clone()).await.unwrap();
    }

    println!("Starting Services...");

    let server_task = server_task(index, &schema, writer.clone(), args.clone());
    let watcher_task = watcher_task(index_dir, writer, args.file_extension.as_str(), &schema);

    tokio::select! {
        _ = server_task => {},
        _ = watcher_task => {},
    }

    Ok(())
}

async fn server_task(index: Index, schema: &Schema, writer: Arc<Mutex<IndexWriter>>, start_args: Arc<Args>) -> Result<()> {
    println!("Starting Server...");
    let middleware = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_timeout_error))
        .timeout(Duration::from_secs(90));

    let app = Router::new()
        .route("/search", get(search_request))
        .route("/index", post(index_request))
        .layer(middleware)
        .layer(Extension(start_args.clone()))
        .layer(Extension(index))
        .layer(Extension(writer))
        .layer(Extension(schema.clone()));

    let addr = SocketAddr::from(([127, 0, 0, 1], start_args.port));

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
    file_extension: &str,
    schema: &Schema,
) -> Result<()> {
    println!("Starting Watcher...");
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let mut debouncer = new_debouncer(Duration::from_secs(10), None, move |res| {
        block_on(async {
            tx.send(res).await.unwrap_or_default();
        })
    })
    .unwrap();

    debouncer
        .watcher()
        .watch(index_dir, RecursiveMode::Recursive)
        .unwrap();

    loop {
        match rx.recv().await {
            Some(event) => {
                let writer = writer.clone();
                match event {
                    Ok(event) => process_index_event(event, writer, file_extension, schema),
                    Err(e) => println!("watch error: {:?}", e),
                }
            }
            None => break,
        }
    }

    Ok(())
}

fn process_index_event(
    events: Vec<DebouncedEvent>,
    writer: Arc<Mutex<IndexWriter>>,
    file_extension: &str,
    schema: &Schema,
) {
    let mut writer = writer.lock().unwrap();
    for event in events {
        let path = event.path;

        println!("{}",  path.as_path().to_str().unwrap());

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == file_extension {
                    if path.exists() {
                        index_file_lines(&schema, &mut writer, &path);
                    } else {
                        delete_dir_files(&schema, &mut writer, &path);
                    }
                }
            }
        } else if path.is_dir() {
            if !path.exists() {
                delete_dir_files(&schema, &mut writer, &path);
            }
        }
    }
    writer.commit().unwrap();
}

fn create_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_facet_field("file_path", FacetOptions::default().set_stored());
    schema_builder.add_text_field("line_content", TEXT | FAST | STORED);
    schema_builder.add_i64_field("line_number", STORED);
    schema_builder.build()
}

fn delete_dir_files(schema: &Schema, writer: &mut IndexWriter, path: &Path) {
    let facet_path = file_path_to_facet_path(&path);
    let facet = Facet::from_path(facet_path);
    let file_path_schema = schema.get_field("file_path").unwrap();
    let term = Term::from_facet(file_path_schema, &facet);
    writer.delete_term(term);
}

fn index_file_lines(schema: &Schema, writer: &mut IndexWriter, path: &PathBuf) {
    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);

    let file_path_schema = schema.get_field("file_path").unwrap();
    let line_content_schema = schema.get_field("line_content").unwrap();
    let line_number_schema = schema.get_field("line_number").unwrap();

    let facet_path = file_path_to_facet_path(&path);

    for (line_number, line) in reader.lines().enumerate() {
        let line = line.unwrap_or(String::default());
        let mut doc = Document::default();

        let line = line.split_whitespace().collect::<Vec<&str>>().join(" ");

        doc.add_facet(file_path_schema, Facet::from_path(&facet_path));
        doc.add_text(line_content_schema, &line);
        doc.add_i64(line_number_schema, line_number as i64);
        writer.add_document(doc).unwrap();
    }
}

fn file_path_to_facet_path(path: &Path) -> Vec<String> {
    path.components()
    .filter_map(|component| match component {
        Component::Normal(os_str) => Some(os_str.to_string_lossy().into_owned()),
        _ => None,
    })
    .collect::<Vec<String>>()
}

async fn index_request(
    schema: Extension<Schema>,
    Extension(startup_args) : Extension<Arc<Args>>,
    writer: Extension<Arc<Mutex<IndexWriter>>>
) -> String {

    let index_dir: &Path = match startup_args.root_path {
        Some(ref path) => path.as_path(),
        None => Path::new("."),
    };

    let dir_map = build_dir_map(index_dir, &startup_args.file_extension).await;

    dir_map.par_iter().for_each(|file| {
        let writer = Arc::clone(&writer);
        let mut writer = writer.lock().unwrap();
        index_file_lines(&schema, &mut writer, file);
    });
    writer.lock().unwrap().commit().unwrap();

    String::new()
}

async fn index_full_dir(index_dir: &Path, file_extension: &str, writer: Arc<Mutex<IndexWriter>>, schema: Arc<Schema>) -> Result<()>{
    println!("Indexing Files...");
    let dir_map = build_dir_map(index_dir, file_extension).await;

    writer.lock().unwrap().delete_all_documents().unwrap();

    dir_map.par_iter().for_each(|file| {
        let writer = Arc::clone(&writer);
        let mut writer = writer.lock().unwrap();
        index_file_lines(&schema, &mut writer, file);
    });
    writer.lock().unwrap().commit()?;

    Ok(())
}


async fn search_request(
    index: Extension<Index>,
    schema: Extension<Schema>,
    Json(request): Json<SearchRequest>,
) -> Json<SearchResult> {
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let query_parser =
        QueryParser::for_index(&index, vec![schema.get_field("line_content").unwrap()]);

    let query = build_query(query_parser, &request);

    let doc_quantity = searcher.search(&query, &Count).unwrap();

    let max_limit = 1000;

    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(max_limit))
        .unwrap();
    let mut results: Vec<Match> = Vec::with_capacity(max_limit);

    let file_path_schema = schema.get_field("file_path").unwrap();
    let line_number_schema = schema.get_field("line_number").unwrap();
    let line_content_schema = schema.get_field("line_content").unwrap();

    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address).unwrap();

        let file_path: String = retrieved_doc
            .get_first(file_path_schema)
            .unwrap()
            .as_facet()
            .unwrap()
            .to_path_string();
        let line_number = retrieved_doc
            .get_first(line_number_schema)
            .unwrap()
            .as_i64()
            .unwrap() as usize;
        let line_content = retrieved_doc
            .get_first(line_content_schema)
            .unwrap()
            .as_text()
            .unwrap()
            .to_string();
        let result = Match {
            file_path,
            line_number,
            line_content,
        };
        results.push(result);
    }

    Json(SearchResult {
        quantity: doc_quantity,
        matches: results,
    })
}

fn build_query(query_parser: QueryParser, request: &SearchRequest) -> Box<dyn Query> {
    let mut query = scape_query(&request.query);

    query = format!("'{}'", query);

    query_parser.parse_query(query.as_str()).unwrap()
}

pub const SPECIAL_CHARS_NO_SPACE: &[char] = &[
    '+', '^', '`', ':', '{', '}', '"', '[', ']', '(', ')', '~', '!', '\\', '*',
];

//Until tantivy doesnt get a latient verison
fn scape_query(query: &String) -> String {
    let mut scrubbed_query = query.to_string();

    for c in SPECIAL_CHARS_NO_SPACE.iter() {
        scrubbed_query = scrubbed_query.replace(*c, &format!("\\{}", c));
    }

    scrubbed_query = scrubbed_query.replace("/", "//");

    let quote_count = scrubbed_query.chars().filter(|&c| c == '\"').count();

    if quote_count % 2 == 1 {
        scrubbed_query = scrubbed_query.replace("\"", "\\\"");
    }

    scrubbed_query
}

async fn build_dir_map(index_dir: &Path, extension: &str) -> Vec<PathBuf> {
    let dir: Vec<String> = SearchBuilder::default()
        .location(index_dir)
        .ext(extension)
        .build()
        .collect();

    dir.par_iter().map(PathBuf::from).collect()
}
