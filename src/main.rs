use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    routing::post,
    BoxError, Extension, Json, Router, Server, response::IntoResponse,
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
    collector::{Count, TopDocs, Collector},
    query::{Query, QueryParser, RegexQuery},
    schema::{Schema, STORED, FacetOptions, Facet, Field, FAST, TEXT},
    Document, Index, IndexWriter, Result, Term, SegmentReader, DocId, Score, DocAddress,
};
use tower::ServiceBuilder;

use futures::executor::block_on;

#[derive(Serialize, Deserialize)]
struct SearchRequest {
    path: String,
    query: String,
    regex: Option<bool>,
    case_sensitive: Option<bool>,
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
    score: f32,
}

#[derive(Serialize, Deserialize)]
struct SearchParseError {
    recieved: String,
    message: String,
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

    let first_index_exist = index_path.join("meta.json").exists();

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
        .route("/search", post(search_request))
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

    let mut debouncer = new_debouncer(Duration::from_secs(5), None, move |res| {
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
        
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == file_extension {
                    delete_file_index(&schema, &mut writer, &path);

                    if path.exists() {
                        index_file_lines(&schema, &mut writer, &path);
                    }
                }
            }
        } else if path.is_dir() {
            if !path.exists() {
                delete_file_index(&schema, &mut writer, &path);
            }
        }
    }
    writer.commit().unwrap();
}

fn create_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    schema_builder.add_facet_field("file_path", FacetOptions::default().set_stored());
    schema_builder.add_text_field("line_index", TEXT);
    schema_builder.add_bytes_field("line_scorer",  FAST);
    schema_builder.add_text_field("line_content", STORED);
    schema_builder.add_i64_field("line_number", STORED);
    schema_builder.build()
}


fn delete_file_index(schema: &Schema, writer: &mut IndexWriter, path: &Path) {
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

    let line_index_schema = schema.get_field("line_index").unwrap();
    let line_scorer_schema = schema.get_field("line_scorer").unwrap();
    let line_content_schema = schema.get_field("line_content").unwrap();
    
    let line_number_schema = schema.get_field("line_number").unwrap();

    let facet_path = file_path_to_facet_path(&path);

    for (line_number, line) in reader.lines().enumerate() {
        let line = line.unwrap_or(String::default());
        let mut doc = Document::default();

        let line = line.split_whitespace().collect::<Vec<&str>>().join(" ");

        doc.add_facet(file_path_schema, Facet::from_path(&facet_path));
        doc.add_text(line_index_schema, &line);
        doc.add_text(line_content_schema, &line);
        doc.add_i64(line_number_schema, line_number as i64);
        doc.add_bytes(line_scorer_schema, to_ascii_string(&line).as_bytes());
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

    let dir_map = build_dir_map(index_dir, &startup_args.file_extension);

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
    let dir_map = build_dir_map(index_dir, file_extension);

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
) -> impl IntoResponse {
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let file_path_schema = schema.get_field("file_path").unwrap();
    let line_number_schema = schema.get_field("line_number").unwrap();
    let line_content_schema = schema.get_field("line_content").unwrap();
    let line_index_schema = schema.get_field("line_index").unwrap();
    let line_scorer_schema = schema.get_field("line_scorer").unwrap();

    let query = match build_query(&index, &request, &line_index_schema) {
        Ok(query) => query,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(SearchParseError{
                recieved: request.query,
                message: e.to_string()
            })).into_response();
        }
    };

    let doc_quantity = searcher.search(&query, &Count).unwrap();


    let max_limit = 1000;

    let top_docs = get_scoring_method(request, max_limit, line_scorer_schema);

    let search_result = searcher
        .search(&query, &top_docs).unwrap();

    let mut results: Vec<Match> = Vec::with_capacity(max_limit);

    for (score, doc_address) in search_result {
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
            score,
        };
        results.push(result);
    }
    (StatusCode::OK, Json(SearchResult {
        quantity: doc_quantity,
        matches: results,
    })).into_response()
}


//This function increase the score of the document if the query is a substring of the line
fn get_scoring_method(request: SearchRequest, max_limit: usize, line_scorer_schema: Field) -> impl Collector<Fruit = Vec<(f32, DocAddress)>> {
    let search_query: String = to_ascii_string(&request.query);

    let top_docs = TopDocs::with_limit(max_limit)
    .tweak_score(move |segment_reader: &SegmentReader| {
        let string_bytes = segment_reader.fast_fields().bytes(line_scorer_schema).unwrap();

        let substring_bytes = search_query.as_bytes().to_owned();

        let bytes_length = substring_bytes.len();

        move |doc: DocId, original_score: Score| {
            (string_bytes.get_bytes(doc)
                .windows(bytes_length)
                .any(|window| window == substring_bytes) as u32 * 10) as Score + original_score
        }
    });
    top_docs
}

fn to_ascii_string(value: &str) -> String {
    value.chars().filter(|c| c.is_ascii()).collect::<String>()
}

fn build_query(index: &Index, request: &SearchRequest, index_schema: &Field) -> Result<Box<dyn Query>> {
    let regex = match &request.regex {
        Some(val) => val,
        None => &false,
    };

    let case_sensitive = match &request.case_sensitive {
        Some(val) => val,
        None => &false,
    };

    if *regex {
        let mut query = String::from(&request.query);

        if !*case_sensitive {
            query = format!("(i+){}", query);
        }

        Ok(Box::new(RegexQuery::from_pattern(&query, *index_schema)?))
    } else {
        let mut query_parser = QueryParser::for_index(&index, vec!(*index_schema));
        query_parser.set_conjunction_by_default();

        let query = scape_query(&request.query);
        
        Ok(query_parser.parse_query(query.as_str())?)
    }
}

pub const SPECIAL_CHARS_SCAPE: &[char] = &[
    '\\', '+', '^', '`', ':', '{', '}', '"', '[', ']', '~', '!', '*', '\"', '(', ')'
];

//Until tantivy doesnt get a latient verison
fn scape_query(query: &String) -> String {
    let mut scrubbed_query = query.to_string();

    for c in SPECIAL_CHARS_SCAPE.iter() {
        scrubbed_query = scrubbed_query.replace(*c, &format!("\\{}", c));
    }

    scrubbed_query = scrubbed_query.replace("/", "//");

    let quote_count = scrubbed_query.chars().filter(|&c| c == '\'').count();

    if quote_count % 2 == 1 {
        scrubbed_query = scrubbed_query.replace("\'", "\\\'");
    }

    scrubbed_query = format!("\"{}\"", scrubbed_query.to_lowercase());

    scrubbed_query
}

fn build_dir_map(index_dir: &Path, extension: &str) -> Vec<PathBuf> {
    let dir: Vec<String> = SearchBuilder::default()
        .location(index_dir)
        .ext(extension)
        .build()
        .collect();

    dir.par_iter().map(PathBuf::from).collect()
}
