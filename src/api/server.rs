use std::convert::Infallible;
use bytes::Bytes;
use std::sync::{Arc};
use serde::{Serialize};
use tokio::sync::{RwLock};
use warp::{Filter, Rejection, Reply, reply};
use warp::http::{StatusCode};
use crate::noctowl::{DocManager, Document};

/*
async fn start_server() {
    let port = env::var("PORT")
        .unwrap_or_else(|_| DEFAULT_PORT.to_string())
        .parse::<u16>()
        .expect("Invalid port number");

    let delete_route = warp::delete()
        .and(warp::path("delete"))
        .and(warp::body::content_length_limit(8000))
        .and(warp::body::form())
        .and_then(delete::delete);

    let routes = delete_route
        .recover(handle_rejection);

    println!("Starting server on port {}...", port);

    let filter = warp::path("v1").and(delete_route);

    let addr = if cfg!(debug_assertions) {
        (Ipv4Addr::UNSPECIFIED, port)
    } else {
        (Ipv4Addr::LOCALHOST, port)
    };

    // Create a broadcast channel for the shutdown signal
    let (tx, rx) = oneshot::channel();

    // Capture the Ctrl+C signal to trigger a shutdown
    let shutdown_signal = tokio::spawn(async {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C signal");
        tx.send(()).expect("Failed to send shutdown signal");
    });

    // Run the server with the provided routes and a closure that captures the shutdown signal
    let (_, server) = warp::serve(api)
        .bind_with_graceful_shutdown(addr, async {
            rx.await.ok();

            println!("Server shutdown gracefully");
        });

    let server_fut = tokio::task::spawn(server);

    // Run the server and the shutdown signal listener concurrently
    let _ = tokio::select! {
    _ = server_fut => println!("Server exited unexpectedly"),
    _ = shutdown_signal => {
            // handle shutdown
        }
    };
}*/

async fn get_doc(doc_manager: Arc<RwLock<DocManager>>, doc_id: &str) ->  Result<Arc<RwLock<Document>>, Box<dyn std::error::Error + Send + Sync>> {
    {
        let doc_manager = doc_manager.read().await;

        if doc_manager.is_doc_cached(&doc_id) {
            return Ok(doc_manager.get_doc_from_cache(&doc_id).clone());
        }
    }

    let load_result = {
        let doc_manager = doc_manager.read().await;

        doc_manager.load_doc_from_disk(&doc_id, -1).await
    };

    return match load_result {
        Ok(doc) => {
            {
                let mut doc_manager = doc_manager.write().await;
                doc_manager.cache_doc(doc);
            }

            {
                let doc_manager = doc_manager.read().await;
                return Ok(doc_manager.get_doc_from_cache(&doc_id).clone());
            }
        },
        Err(m) => Err(m)
    }
}

fn with_doc_manager(
    doc_manager: Arc<RwLock<DocManager>>,
) -> impl Filter<Extract=(Arc<RwLock<DocManager>>, ), Error=Infallible> + Clone {
    warp::any().map(move || doc_manager.clone())
}

pub async fn create_doc_filter(
    doc_manager: Arc<RwLock<DocManager>>
) -> Result<impl Reply, Rejection> {
    let mut doc_manager = doc_manager.write().await;

    match doc_manager.create_doc(true).await {
        Some(doc_id) => Ok(reply::with_status(
            reply::json(&doc_id),
            StatusCode::CREATED,
        )),
        None => Err(warp::reject::custom(Error {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            messages: vec!["Failed to create document".to_string()],
        }))
    }
}

pub async fn get_doc_filter(
    doc_id: String,
    doc_manager: Arc<RwLock<DocManager>>
) -> Result<impl Reply, Rejection> {
    match get_doc(doc_manager, &doc_id).await {
        Ok(doc) => {
            let cached_doc = doc.read().await;
            let doc_state = cached_doc.get_doc_state();

            Ok(reply::with_status(
                reply::with_header(doc_state, "Content-Type", "application/octet-stream"),
                StatusCode::OK,
            ))
        }
        // todo: handle other kinds of errors and internal server errors
        Err(_) => Err(warp::reject::custom(Error {
            code: StatusCode::NOT_FOUND,
            messages: vec!["Document not found".to_string()],
        }))
    }
}

pub async fn update_doc_filter(
    doc_id: String,
    body: Bytes,
    doc_manager: Arc<RwLock<DocManager>>
) -> Result<impl Reply, Rejection> {
    let update: Vec<u8> = body.iter().map(|b| *b).collect();

    return match get_doc(doc_manager.clone(), &doc_id).await {
        Ok(doc) => {
            let doc_manager = doc_manager.read().await;
            let mut doc = doc.write().await;

            match doc_manager.update_doc(&doc_id, &mut doc, &update).await {
                Ok(()) => Ok(reply::with_status(
                    reply::json(&{}),
                    StatusCode::OK,
                )),
                Err(e) => Err(warp::reject::custom(Error {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    messages: vec![format!("Error {}, docId: {}", e, doc_id)],
                }))
            }
        }
        // todo: handle other kinds of errors and internal server errors
        Err(_) => Err(warp::reject::custom(Error {
            code: StatusCode::NOT_FOUND,
            messages: vec!["Document not found".to_string()],
        }))
    };
}

pub fn create_routes(doc_manager: Arc<RwLock<DocManager>>) -> impl Filter<Extract = impl Reply, Error = Infallible> + Clone {
    let create_doc_filter = warp::path!("doc")
        .and(warp::post())
        .and(with_doc_manager(doc_manager.clone()))
        .and_then(create_doc_filter);

    let get_doc_filter = warp::path!("doc" / String)
        .and(warp::get())
        .and(with_doc_manager(doc_manager.clone()))
        .and_then(get_doc_filter);

    let update_doc_filter = warp::path!("doc" / String / "update")
        .and(warp::post())
        .and(warp::header::exact("content-type", "application/octet-stream"))
        .and(warp::body::content_length_limit(10 * 1024 * 1024))
        .and(warp::body::bytes())
        .and(with_doc_manager(doc_manager.clone()))
        .and_then(update_doc_filter);

    create_doc_filter
        .or(get_doc_filter)
        .or(update_doc_filter)
        .recover(handle_rejection)
}

#[derive(Debug)]
pub struct Error {
    pub code: StatusCode,
    pub messages: Vec<String>,
}

impl warp::reject::Reject for Error {}

impl Error {
    pub fn single(code: StatusCode, message: &str) -> Error {
        Error {
            code,
            messages: vec![message.to_string()],
        }
    }

    pub fn bad_request_single(message: &str) -> Error {
        Error::single(StatusCode::BAD_REQUEST, message)
    }
}

#[derive(Serialize)]
pub struct ErrorMessage {
    pub errors: Vec<String>,
}

pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code: StatusCode;
    let error_messages: Vec<String>;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        error_messages = vec!["NOT_FOUND".to_string()];
    } else if let Some(error) = err.find::<Error>() {
        code = error.code;
        error_messages = error.messages.clone();
    } else {
        eprintln!("unhandled rejection: {:?}", err);

        code = StatusCode::INTERNAL_SERVER_ERROR;
        error_messages = vec!["INTERNAL_SERVER_ERROR".to_string()];
    }

    let json = reply::json(&ErrorMessage {
        errors: error_messages,
    });

    Ok(reply::with_status(json, code))
}
