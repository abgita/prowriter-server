use std::convert::Infallible;
use std::sync::Arc;

use serde::Serialize;
use warp::{Filter, Rejection, Reply, reply};
use warp::http::StatusCode;

use crate::noctowl::doc_manager::{DocManager};

use crate::api::routes::{
	create_doc,
	get_doc,
	update_doc,
	get_snapshots,
};

fn with_doc_manager(
	doc_manager: Arc<DocManager>,
) -> impl Filter<Extract=(Arc<DocManager>, ), Error=Infallible> + Clone {
	warp::any().map(move || doc_manager.clone())
}

pub fn create_routes(
	doc_manager: Arc<DocManager>
) -> impl Filter<Extract=impl Reply, Error=Infallible> + Clone {
	let create_doc_filter = warp::path!("doc")
		.and(warp::post())
		.and(with_doc_manager(doc_manager.clone()))
		.and_then(create_doc::create_doc_filter);

	let get_doc_filter = warp::path!("doc" / String / i64)
		.and(warp::get())
		.and(with_doc_manager(doc_manager.clone()))
		.and_then(get_doc::get_doc_filter);

	let update_doc_filter = warp::path!("doc" / String / "update")
		.and(warp::post())
		.and(warp::header::exact("content-type", "application/octet-stream"))
		.and(warp::body::content_length_limit(10 * 1024 * 1024))
		.and(warp::body::bytes())
		.and(with_doc_manager(doc_manager.clone()))
		.and_then(update_doc::update_doc_filter);

	let get_doc_snaps_filter = warp::path!("doc" / String / "snapshots")
		.and(with_doc_manager(doc_manager.clone()))
		.and_then(get_snapshots::get_snapshots_filter);

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
