use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde_json::json;
use tokio::sync::RwLock;
use warp::{Filter, Rejection, Reply, reply};
use warp::http::StatusCode;

use crate::noctowl::{DocManager, Document};
use crate::noctowl::storage::{SnapshotInfo};

async fn get_doc(
	doc_manager: Arc<DocManager>,
	doc_id: &str,
) -> Result<Arc<RwLock<Document>>, Box<dyn std::error::Error + Send + Sync>> {
	if doc_manager.is_doc_cached(&doc_id).await {
		return Ok(doc_manager.get_doc_from_cache(&doc_id).await);
	}

	let load_result = doc_manager.load_doc_from_disk(&doc_id, -1).await;

	return match load_result {
		Ok(doc) => {
			doc_manager.cache_doc(doc).await;

			return Ok(doc_manager.get_doc_from_cache(&doc_id).await);
		}
		Err(m) => Err(m)
	};
}

fn with_doc_manager(
	doc_manager: Arc<DocManager>,
) -> impl Filter<Extract=(Arc<DocManager>, ), Error=Infallible> + Clone {
	warp::any().map(move || doc_manager.clone())
}

pub async fn create_doc_filter(
	doc_manager: Arc<DocManager>
) -> Result<impl Reply, Rejection> {
	match doc_manager.create_doc(true).await {
		Some(doc_id) => Ok(reply::with_status(
			reply::json(&json!({
                "docId": doc_id
            })),
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
	snapshot_id: i64,
	doc_manager: Arc<DocManager>,
) -> Result<impl Reply, Rejection> {
	if snapshot_id != -1 {
		let load_result = doc_manager.load_doc_from_disk(&doc_id, snapshot_id).await;

		return match load_result {
			Ok(doc) => {
				{
					let doc_state = doc.get_doc_state();

					Ok(reply::with_status(
						reply::with_header(
							doc_state,
							"Content-Type",
							"application/octet-stream",
						),
						StatusCode::OK,
					))
				}
			}
			Err(_) => Err(warp::reject::custom(Error {
				code: StatusCode::NOT_FOUND,
				messages: vec!["Document not found".to_string()],
			}))
		};
	}

	match get_doc(doc_manager, &doc_id).await {
		Ok(doc) => {
			let cached_doc = doc.read().await;
			let doc_state = cached_doc.get_doc_state();

			Ok(reply::with_status(
				reply::with_header(
					doc_state,
					"Content-Type",
					"application/octet-stream",
				),
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

pub async fn get_snapshots_filter(
	doc_id: String,
	doc_manager: Arc<DocManager>,
) -> Result<impl Reply, Rejection> {
	match doc_manager.get_snapshot_list(&doc_id, 10).await {
		Ok(list) => {
			#[derive(Serialize)]
			struct SnapshotList {
				list: Vec<SnapshotInfo>,
			}

			Ok(reply::with_status(
				reply::json(&SnapshotList {
					list
				}),
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
	doc_manager: Arc<DocManager>,
) -> Result<impl Reply, Rejection> {
	let update: Vec<u8> = body.iter().map(|b| *b).collect();

	return match get_doc(doc_manager.clone(), &doc_id).await {
		Ok(doc) => {
			let mut doc = doc.write().await;

			match doc_manager.update_doc(&doc_id, &mut doc, &update).await {
				Ok(()) => Ok(reply::with_status(
					reply::json(&{}),
					StatusCode::OK,
				)),
				Err(e) => {
					return Err(warp::reject::custom(Error {
						code: StatusCode::INTERNAL_SERVER_ERROR,
						messages: vec![format!("Error {}, docId: {}", e, doc_id)],
					}));
				}
			}
		}
		// todo: handle other kinds of errors and internal server errors
		Err(_) => Err(warp::reject::custom(Error {
			code: StatusCode::NOT_FOUND,
			messages: vec!["Document not found".to_string()],
		}))
	};
}

pub fn create_routes(
	doc_manager: Arc<DocManager>
) -> impl Filter<Extract=impl Reply, Error=Infallible> + Clone {
	let create_doc_filter = warp::path!("doc")
		.and(warp::post())
		.and(with_doc_manager(doc_manager.clone()))
		.and_then(create_doc_filter);

	let get_doc_filter = warp::path!("doc" / String / i64)
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

	let get_doc_snaps_filter = warp::path!("doc" / String / "snapshots")
		.and(with_doc_manager(doc_manager.clone()))
		.and_then(get_snapshots_filter);

	create_doc_filter
		.or(get_doc_filter)
		.or(update_doc_filter)
		.or(get_doc_snaps_filter)
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
