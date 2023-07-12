use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;
use crate::accounts::jwt::AuthUser;

use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlStatus;
use crate::slog;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NewProjectRequest {
	pub project_name: String,
}

#[derive(Serialize)]
pub struct NewProjectResponse {
	pub project_pid: String,
}

pub async fn create_project(
	au: AuthUser,
	noctowl: Arc<Noctowl>,
	request: NewProjectRequest,
) -> Result<WithStatus<Json>, Rejection> {
	let user_pid = au.user_pid;
	let project_name = request.project_name;

	if project_name.is_empty() {
		return Err(custom(CustomError::single(StatusCode::BAD_REQUEST, "project_name cannot be empty")));
	}

	let (project_pid, status) = noctowl.create_project(&user_pid, project_name.as_str()).await
		.map_err(|e| {
			slog!("Failed to create project: {}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create project"))
		})?;

	if status != NoctowlStatus::Ok {
		if status == NoctowlStatus::ProjectAlreadyExists {
			return Err(custom(CustomError::single(StatusCode::CONFLICT, "Project already exists")));
		}

		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create project")));
	}

	Ok(reply::with_status(
		reply::json(&NewProjectResponse {
			project_pid,
		}),
		StatusCode::CREATED,
	))
}
