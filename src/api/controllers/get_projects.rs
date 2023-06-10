use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;
use crate::{slog};

use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectResponse {
	pub project_pid: String,
	pub name: String,
	pub created_at: i64,
	pub last_accessed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectsResponse {
	pub projects: Vec<ProjectResponse>,
}

pub async fn get_projects(
	user_pid: String,
	noctowl: Arc<Noctowl>
) -> Result<WithStatus<Json>, Rejection> {

	let (projects, status) = noctowl.get_projects(&user_pid).await
		.map_err(|e| {
			slog!("Error getting projects: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get projects"))
		})?;

	if status != NoctowlStatus::Ok {
		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get projects")));
	}

	let project_list = projects.iter().map(|project| {
		ProjectResponse {
			project_pid: project.project_pid.clone(),
			name: project.name.clone(),
			created_at: project.created_at,
			last_accessed: project.last_accessed,
		}
	}).collect::<Vec<ProjectResponse>>();

	Ok(reply::with_status(
		reply::json(&ProjectsResponse {
			projects: project_list,
		}),
		StatusCode::OK,
	))
}
