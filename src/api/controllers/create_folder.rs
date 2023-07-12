use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;
use crate::accounts::jwt::AuthUser;
use crate::api::controllers::get_project::FolderResponse;

use crate::noctowl::lib::Noctowl;
use crate::noctowl::lib::constants::{ICON_STRING_MAX_LENGTH, MIN_FOLDER_ID, PROJECT_PID_LENGTH};
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
	au: AuthUser,
	noctowl: Arc<Noctowl>,
	request: NewFolderRequest,
) -> Result<WithStatus<Json>, Rejection> {
	let user_pid = au.user_pid;

	let NewFolderRequest {
		folder_name,
		folder_icon,
		parent_folder_id,
	} = request;

	if project_pid.len() != PROJECT_PID_LENGTH {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "Invalid project_pid")));
	}

	if folder_name.is_empty() {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "folder_name cannot be empty")));
	}

	if let Some(parent_folder_id) = parent_folder_id {
		if parent_folder_id < MIN_FOLDER_ID {
			let message: String = format!("Invalid parent_folder_id. Min value is {}", MIN_FOLDER_ID);

			return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, message.as_str())));
		}
	}

	if let Some(folder_icon) = folder_icon.clone() {
		if folder_icon.len() > ICON_STRING_MAX_LENGTH {
			let message: String = format!("Invalid folder_icon. Max length is {}", ICON_STRING_MAX_LENGTH);

			return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, message.as_str())));
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
			parent_folder_id: folder_row.parent_folder_id,
			locked: folder_row.locked == 1,
			position: folder_row.position,
		}),
		StatusCode::CREATED,
	))
}
