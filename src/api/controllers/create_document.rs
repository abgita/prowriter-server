use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;
use crate::api::controllers::get_project::DocResponse;

use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlStatus;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NewDocRequest {
	pub doc_name: String,
	pub doc_icon: Option<String>,
	pub folder_id: Option<i32>,
}

pub async fn create_document(
	project_pid: String,
	user_pid: String,
	noctowl: Arc<Noctowl>,
	request: NewDocRequest,
) -> Result<WithStatus<Json>, Rejection> {
	let NewDocRequest {
		doc_name,
		doc_icon,
		folder_id,
	} = request;

	let (doc_row, status) = noctowl.create_document(
		&user_pid,
		&project_pid,
		&doc_name,
		doc_icon,
		folder_id,
	).await
		.map_err(|e| {
			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create folder"))
		})?;

	if status != NoctowlStatus::Ok {
		return Err(
			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create folder"))
		);
	}

	// at this point we know it is some, since it would return
	// status=NoctowlStatus::DocumentAlreadyExists otherwise
	// TODO: refactor Noctowl::create_document to not check for the existence of the document
	let doc_row = doc_row.unwrap();

	Ok(reply::with_status(
		reply::json(&DocResponse {
			doc_pid: doc_row.doc_pid,
			folder_id: doc_row.folder_id,
			name: doc_row.name,
			icon: doc_row.icon,
			locked: doc_row.locked == 1,
			created_at: doc_row.created_at,
			last_accessed: doc_row.last_accessed,
			position: doc_row.position,
		}),
		StatusCode::CREATED,
	))
}
