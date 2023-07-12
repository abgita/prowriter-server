use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithHeader, WithStatus};
use crate::accounts::CustomError;
use crate::accounts::jwt::AuthUser;
use crate::noctowl::db_document::DocUpdateInfo;

use crate::noctowl::lib::{Noctowl};
use crate::noctowl::lib::constants::{DOC_PID_LENGTH, PROJECT_PID_LENGTH};
use crate::noctowl::NoctowlStatus;
use crate::slog;

#[derive(Deserialize, Serialize)]
pub struct GetDocumentParams {
	pub u: Option<i64>,
}

#[derive(Deserialize, Serialize)]
pub struct GetDocumentRevisionsParams {
	pub o: i64,
	pub l: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetDocumentRevisionsResponse {
	pub revisions: Vec<DocUpdateInfo>
}

pub async fn get_document(
	project_pid: String,
	doc_pid: String,
	query: GetDocumentParams,
	au: AuthUser,
	noctowl: Arc<Noctowl>,
) -> Result<WithStatus<WithHeader<Vec<u8>>>, Rejection> {
	let user_pid = au.user_pid;
	let update_id = query.u;

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
		update_id
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

pub async fn get_document_revisions(
	project_pid: String,
	doc_pid: String,
	query: GetDocumentRevisionsParams,
	au: AuthUser,
	noctowl: Arc<Noctowl>,
) -> Result<WithStatus<Json>, Rejection> {
	let user_pid = au.user_pid;

	let offset = query.o;
	let limit = query.l;

	if project_pid.len() != PROJECT_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid project_pid")));
	}

	if doc_pid.len() != DOC_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid doc_pid")));
	}

	let (updates, status) = noctowl.get_updates_with_offset_limit(
		&user_pid,
		&project_pid,
		&doc_pid,
		offset,
		limit
	).await
		.map_err(|e| {
			slog!("Error getting document updates: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get the revisions for document"))
		})?;

	if status != NoctowlStatus::Ok {
		if status == NoctowlStatus::DocumentNotFound {
			return Err(custom(CustomError::single(StatusCode::NOT_FOUND, "revisions for document not found")));
		}

		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get the revisions for document")));
	}

	let updates = updates.unwrap();

	Ok(reply::with_status(
		reply::json(&GetDocumentRevisionsResponse {
			revisions: updates
		}),
		StatusCode::OK,
	))
}
