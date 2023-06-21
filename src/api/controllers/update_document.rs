use std::sync::Arc;

use serde::{Serialize};
use bytes::Bytes;
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{WithHeader, WithStatus};
use crate::accounts::CustomError;
use crate::noctowl::document::YrsUpdateStatus;

use crate::noctowl::lib::{Noctowl};
use crate::{nlog, slog};

#[derive(Serialize)]
pub struct UpdateDocumentResponse {
	pub sv: Option<Vec<u8>>,
}

pub async fn update_document(
	project_pid: String,
	doc_pid: String,
	user_pid: String,
	noctowl: Arc<Noctowl>,
	body: Bytes
) -> Result<WithStatus<WithHeader<Vec<u8>>>, Rejection> {
	let update: Vec<u8> = body.iter().map(|b| *b).collect();

	let status = noctowl.update_document(&user_pid, &project_pid, &doc_pid, update).await
		.map_err(|e| {
			slog!("Error updating document: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to update document"))
		})?;

	nlog!(
		"PUT:api/v1/projects/{}/docs/{}/update - {:?}",
		project_pid,
		doc_pid,
		status
	);

	if status == YrsUpdateStatus::Failed {
		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to update document")));
	}

	Ok(reply::with_status(
		reply::with_header(
			match status.clone() {
				YrsUpdateStatus::Pending(sv) => sv,
				_ => vec![],
			},
			"Content-Type",
			"application/octet-stream",
		),
		match status {
			YrsUpdateStatus::NoUpdate => StatusCode::NOT_MODIFIED,
			YrsUpdateStatus::Pending(_) => StatusCode::CONFLICT,
			_ => StatusCode::OK,
		}
	))
}
