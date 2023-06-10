use std::sync::Arc;

use bytes::Bytes;
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;

use crate::noctowl::lib::{Noctowl};
use crate::noctowl::NoctowlStatus;
use crate::slog;

pub async fn update_document(
	project_pid: String,
	doc_pid: String,
	user_pid: String,
	noctowl: Arc<Noctowl>,
	body: Bytes
) -> Result<WithStatus<Json>, Rejection> {
	let update: Vec<u8> = body.iter().map(|b| *b).collect();

	let status = noctowl.update_document(&user_pid, &project_pid, &doc_pid, update).await
		.map_err(|e| {
			slog!("Error updating document: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to update document"))
		})?;

	if status != NoctowlStatus::Ok {
		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to update document")));
	}

	Ok(reply::with_status(
		reply::json(&{}),
		StatusCode::OK,
	))
}
