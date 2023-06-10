use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;
use crate::api::controllers::get_project::FolderResponse;

use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlStatus;
use crate::slog;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NewFolderRequest {
	pub folder_name: String,
	pub folder_icon: Option<String>,
	pub parent_folder_id: Option<i32>,
}

pub async fn create_folder(
	project_pid: String,
	user_pid: String,
	noctowl: Arc<Noctowl>,
	request: NewFolderRequest,
) -> Result<WithStatus<Json>, Rejection> {
	let NewFolderRequest {
		folder_name,
		folder_icon,
		parent_folder_id,
	} = request;

	if let Some(parent_folder_id) = parent_folder_id {
		if parent_folder_id < 2 {
			return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid parent folder ID. Min value is 2")));
		}
	}

	let (folder_row, status) = noctowl.create_folder(
		&user_pid,
		&project_pid,
		&folder_name,
		folder_icon.clone(),
		parent_folder_id,
	).await
		.map_err(|e| {
			slog!("Error creating folder: {}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create folder"))
		})?;

	if status != NoctowlStatus::Ok {
		slog!("Error creating folder: {:?}", status);

		return Err(
			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create folder"))
		);
	}

	Ok(reply::with_status(
		reply::json(&FolderResponse {
			folder_id: folder_row.folder_id,
			folder_name,
			folder_icon,
			parent_folder_id,
			locked: folder_row.locked == 1,
			position: folder_row.position,
		}),
		StatusCode::CREATED,
	))
}
