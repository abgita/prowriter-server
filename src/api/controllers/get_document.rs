use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{WithHeader, WithStatus};
use crate::accounts::CustomError;

use crate::noctowl::lib::{Noctowl};
use crate::noctowl::lib::constants::{DOC_PID_LENGTH, PROJECT_PID_LENGTH};
use crate::noctowl::NoctowlStatus;
use crate::slog;

#[derive(Deserialize, Serialize)]
pub struct GetDocumentParams {
	pub s: Option<i64>,
	pub u: Option<i64>,
}

pub async fn get_document(
	project_pid: String,
	doc_pid: String,
	query: GetDocumentParams,
	user_pid: String,
	noctowl: Arc<Noctowl>,
) -> Result<WithStatus<WithHeader<Vec<u8>>>, Rejection> {
	let snapshot_id = query.s;
	let update_index = query.u;

	if project_pid.len() != PROJECT_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid project_pid")));
	}

	if doc_pid.len() != DOC_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid doc_pid")));
	}

	let (document, status) = noctowl.get_document(
		&user_pid,
		&project_pid,
		&doc_pid,
		snapshot_id,
		update_index
	).await
		.map_err(|e| {
			slog!("Error getting document: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get document"))
		})?;

	if status != NoctowlStatus::Ok {
		if status == NoctowlStatus::DocumentNotFound {
			return Err(custom(CustomError::single(StatusCode::NOT_FOUND, "Document not found")));
		}

		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get document")));
	}

	let document = document.unwrap();

	let doc_state = {
		let document = document.lock().await;

		document.get_doc_state()
	};

	Ok(reply::with_status(
		reply::with_header(
			doc_state,
			"Content-Type",
			"application/octet-stream",
		),
		StatusCode::OK,
	))
}

pub async fn get_prev_document(
	project_pid: String,
	doc_pid: String,
	prev_amount: i32,
	user_pid: String,
	noctowl: Arc<Noctowl>,
) -> Result<WithStatus<WithHeader<Vec<u8>>>, Rejection> {
	if project_pid.len() != PROJECT_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid project_pid")));
	}

	if doc_pid.len() != DOC_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid doc_pid")));
	}

	if prev_amount < 0 || prev_amount > 256 {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid prev_amount")));
	}

	let (document, status) = noctowl.get_prev_document(
		&user_pid,
		&project_pid,
		&doc_pid,
		prev_amount
	).await
		.map_err(|e| {
			slog!("Error getting document: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get document"))
		})?;

	if status != NoctowlStatus::Ok {
		if status == NoctowlStatus::DocumentNotFound {
			return Err(custom(CustomError::single(StatusCode::NOT_FOUND, "Document not found")));
		}

		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get document")));
	}

	let document = document.unwrap();

	let doc_state = {
		let document = document.lock().await;

		document.get_doc_state()
	};

	Ok(reply::with_status(
		reply::with_header(
			doc_state,
			"Content-Type",
			"application/octet-stream",
		),
		StatusCode::OK,
	))
}
