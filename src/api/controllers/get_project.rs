use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::{ Rejection, reply};
use warp::http::StatusCode;
use warp::reject::custom;
use warp::reply::{Json, WithStatus};
use crate::accounts::CustomError;

use crate::noctowl::lib::{Noctowl};
use crate::noctowl::NoctowlStatus;
use crate::slog;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectResponse {
	pub project_pid: String,
	pub name: String,
	pub created_at: i64,
	pub last_accessed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderResponse {
	pub folder_id: i32,
	pub folder_name: String,
	pub folder_icon: Option<String>,
	pub parent_folder_id: Option<i32>,
	pub locked: bool,
	pub position: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocResponse {
	pub doc_pid: String,
	pub folder_id: i32,
	pub name: String,
	pub icon: Option<String>,
	pub locked: bool,
	pub created_at: i64,
	pub last_accessed: i64,
	pub position: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderTree {
	pub folder: FolderResponse,
	pub children: Vec<FolderTree>,
	pub docs: Vec<DocResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetProjectResponse {
	pub project: ProjectResponse,
	pub folders: Vec<FolderResponse>,
	pub docs: Vec<DocResponse>,
}

pub async fn get_project(
	project_pid: String,
	user_pid: String,
	noctowl: Arc<Noctowl>,
) -> Result<WithStatus<Json>, Rejection> {
	let (content, status) = noctowl.get_project(&user_pid, &project_pid).await
		.map_err(|e| {
			slog!("Error getting project: {:?}", e);

			custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get project"))
		})?;

	if status != NoctowlStatus::Ok {
		if status == NoctowlStatus::ProjectNotFound {
			return Err(custom(CustomError::single(StatusCode::NOT_FOUND, "Project not found")));
		}

		return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get project")));
	}

	let content = content.unwrap();

	let project = ProjectResponse {
		project_pid: content.project.project_pid,
		name: content.project.name,
		created_at: content.project.created_at,
		last_accessed: content.project.last_accessed,
	};

	let folders = content.folders.iter().map(|f| {
		FolderResponse {
			folder_id: f.folder_id,
			folder_name: f.folder_name.clone(),
			folder_icon: f.folder_icon.clone(),
			parent_folder_id: f.parent_folder_id,
			locked: f.locked == 1,
			position: f.position,
		}
	}).collect::<Vec<FolderResponse>>();

	let docs = content.docs.iter().map(|d| {
		DocResponse {
			doc_pid: d.doc_pid.clone(),
			folder_id: d.folder_id,
			name: d.name.clone(),
			icon: d.icon.clone(),
			locked: d.locked == 1,
			created_at: d.created_at,
			last_accessed: d.last_accessed,
			position: d.position,
		}
	}).collect::<Vec<DocResponse>>();

	Ok(reply::with_status(
		reply::json(&GetProjectResponse {
			project,
			folders,
			docs,
		}),
		StatusCode::OK,
	))
}
