use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};

use crate::accounts::CustomError;
use crate::noctowl::lib::constants::{DOC_PID_LENGTH, PROJECT_PID_LENGTH};
use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlStatus;
use crate::slog;

#[derive(Deserialize, Serialize)]
pub struct RestoreDocumentParams {
	pub u: i64,
}

pub async fn restore_document(
	project_pid: String,
	doc_pid: String,
	query: RestoreDocumentParams,
	user_pid: String,
	noctowl: Arc<Noctowl>,
) -> Result<WithStatus<Json>, Rejection> {
	let update_id = query.u;

	if project_pid.len() != PROJECT_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid project_pid")));
	}

	if doc_pid.len() != DOC_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid doc_pid")));
	}

	if update_id < 0 {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid request")));
	}

	let (_, status) = noctowl.restore_document(
		&user_pid,
		&project_pid,
		&doc_pid,
		update_id,
	).await
		.map_err(|e| {
			slog!("Error restore document: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to restore document"))
		})?;

	if status != NoctowlStatus::Ok {
		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to restore document")));
	}

	Ok(reply::with_status(
		reply::json(
			&{}
		),
		StatusCode::OK,
	))
}
