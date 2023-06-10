use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::common::utils;
use crate::noctowl::{NoctowlError, NoctowlStatus};
use crate::noctowl::db_document::{get_doc_db_connection, get_document, process_update};
use crate::noctowl::db_main::{create_project, get_project_by_pid, get_projects_by_user_pid, load_main_db, Project};
use crate::noctowl::db_project::{create_document, DocRow, FolderRow, get_project_content, get_project_db_connection, insert_folder};
use crate::noctowl::document::Document;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectContent {
	pub project: Project,
	pub folders: Vec<FolderRow>,
	pub docs: Vec<DocRow>,
}

#[derive(Clone)]
pub struct NoctowlOptions {
	pub storage_dir: String,
	pub users_dir: String,
}

#[derive(Clone)]
pub struct Noctowl {
	options: NoctowlOptions,
	main_db: SqlitePool,
	connection_pools: Arc<RwLock<HashMap<String, SqlitePool>>>,
	docs_cache: Arc<RwLock<HashMap<String, Arc<Mutex<Document>>>>>,
	access_map: Arc<RwLock<HashMap<String, Instant>>>,
}

impl Noctowl {
	pub async fn new(storage_dir: Option<String>) -> Result<Self, NoctowlError> {
		let storage_dir = storage_dir.unwrap_or(".storage".to_string());
		let users_dir = format!("{}/users", storage_dir);

		let path = PathBuf::from(&storage_dir);
		let path = path.as_path();

		utils::create_dirs_if_not_exists(path).unwrap();

		// --------------------------------------------------------- create the main db and directories

		// before creating a new database we must ensure we don't have a remote copy of the db
		// in the cloud. If so we should download it first
		let main_db = load_main_db(&format!("{}/main.sqlite", &storage_dir)).await
			.map_err(|e| NoctowlError::Error(
				"Error loading main database",
				Box::new(e),
			))?;

		// ---------------------------------------------------------------------- create database pools

		let connection_pools = Arc::new(RwLock::new(HashMap::<String, SqlitePool>::new()));

		// ---------------------------------------------------------------------- create document cache

		let docs_cache = Arc::new(RwLock::new(HashMap::<String, Arc<Mutex<Document>>>::new()));

		Ok(Noctowl {
			options: NoctowlOptions {
				storage_dir,
				users_dir,
			},
			main_db,
			connection_pools,
			docs_cache,
			access_map: Arc::new(RwLock::new(HashMap::<String, Instant>::new())),
		})
	}

	pub async fn create_project(
		&self,
		user_pid: &str,
		project_name: &str,
	) -> Result<(String, NoctowlStatus), NoctowlError> {
		let project_pid = format!("p{}", utils::get_short_pid());

		create_project(
			&self.main_db,
			&self.options.users_dir,
			&user_pid,
			&project_pid,
			&project_name,
		).await?;

		Ok((project_pid, NoctowlStatus::Ok))
	}

	pub async fn create_folder(
		&self,
		user_pid: &str,
		project_pid: &str,
		folder_name: &str,
		folder_icon: Option<String>,
		parent_folder_id: Option<i32>,
	) -> Result<(FolderRow, NoctowlStatus), NoctowlError> {
		let project_connection = get_project_db_connection(
			&self.main_db,
			&self.connection_pools,
			&self.access_map,
			&self.options.users_dir,
			&user_pid,
			&project_pid,
		).await?;

		let result = insert_folder(
			&project_connection,
			&project_pid,
			&folder_name,
			folder_icon,
			parent_folder_id,
		).await?;

		Ok((result, NoctowlStatus::Ok))
	}

	// todo: refactor this to not check for the existence of the document. delegate this to the caller
	//   so that the result is more consistent
	pub async fn create_document(
		&self,
		user_pid: &str,
		project_pid: &str,
		doc_name: &str,
		doc_icon: Option<String>,
		folder_id: Option<i32>,
	) -> Result<(Option<DocRow>, NoctowlStatus), NoctowlError> {
		let project_connection = get_project_db_connection(
			&self.main_db,
			&self.connection_pools,
			&self.access_map,
			&self.options.users_dir,
			&user_pid,
			&project_pid,
		).await?;

		let doc_pid = utils::get_new_uuid();

		let doc_row = create_document(
			&project_connection,
			&self.options.users_dir,
			&doc_pid,
			&user_pid,
			&project_pid,
			&doc_name,
			doc_icon,
			folder_id,
		).await?;

		if doc_row.is_none() {
			return Ok((None, NoctowlStatus::DocumentAlreadyExists));
		}

		Ok((doc_row, NoctowlStatus::Ok))
	}

	pub async fn get_project(
		&self,
		user_pid: &str,
		project_pid: &str,
	) -> Result<(Option<ProjectContent>, NoctowlStatus), NoctowlError> {
		let project = get_project_by_pid(&self.main_db, &project_pid).await?;

		if project.is_none() {
			return Ok((None, NoctowlStatus::ProjectNotFound));
		}

		let project_connection = get_project_db_connection(
			&self.main_db,
			&self.connection_pools,
			&self.access_map,
			&self.options.users_dir,
			&user_pid,
			&project_pid,
		).await?;

		let (folders, docs) = get_project_content(&project_connection, &project_pid).await?;

		let project_content = ProjectContent {
			project: project.unwrap(),
			folders,
			docs,
		};

		Ok((Some(project_content), NoctowlStatus::Ok))
	}

	pub async fn get_projects(
		&self,
		user_pid: &str,
	) -> Result<(Vec<Project>, NoctowlStatus), NoctowlError> {
		let project = get_projects_by_user_pid(&self.main_db, &user_pid).await?;

		Ok((project, NoctowlStatus::Ok))
	}

	pub async fn get_document(
		&self,
		user_pid: &str,
		project_pid: &str,
		doc_pid: &str,
		snapshot_id: Option<i64>,
		update_index: Option<i64>,
	) -> Result<(Option<Arc<Mutex<Document>>>, NoctowlStatus), NoctowlError> {
		let project_connection = get_project_db_connection(
			&self.main_db,
			&self.connection_pools,
			&self.access_map,
			&self.options.users_dir,
			&user_pid,
			&project_pid,
		).await?;

		let doc_connection = get_doc_db_connection(
			&project_connection,
			&self.connection_pools,
			&self.access_map,
			&self.options.users_dir,
			&doc_pid,
			&user_pid,
		).await?;

		match get_document(
			&doc_connection,
			&self.docs_cache,
			&doc_pid,
			&snapshot_id,
			&update_index,
		).await {
			Ok(doc) => Ok((Some(doc), NoctowlStatus::Ok)),
			Err(e) => match e {
				NoctowlError::DocumentNotFound(_) => Ok((None, NoctowlStatus::DocumentNotFound)),
				_ => Err(e),
			}
		}
	}

	pub async fn update_document(
		&self,
		user_pid: &str,
		project_pid: &str,
		doc_pid: &str,
		update: Vec<u8>,
	) -> Result<NoctowlStatus, NoctowlError> {
		let project_connection = get_project_db_connection(
			&self.main_db,
			&self.connection_pools,
			&self.access_map,
			&self.options.users_dir,
			&user_pid,
			&project_pid,
		).await?;

		process_update(
			&update,
			&self.docs_cache,
			&self.access_map,
			&self.options.users_dir,
			&doc_pid,
			&user_pid,
			&project_pid,
			&project_connection,
			&self.connection_pools,
		).await?;

		Ok(NoctowlStatus::Ok)
	}

	pub async fn clean_up(&self) {
		let mut connection_pools = self.connection_pools.write().await;

		for (_, pool) in connection_pools.iter_mut() {
			pool.close().await;
		}
		connection_pools.clear();

		self.docs_cache.write().await.clear();
		self.main_db.close().await;
	}

	pub async fn clean_up_stale_connections_task(&self) {
		let sleep_duration = Duration::from_secs(60 * 15);
		let expiration_duration = Duration::from_secs(60 * 30);

		loop {
			tokio::time::sleep(sleep_duration).await;

			let cloned_map = {
				let map = self.access_map.read().await;
				map.clone()
			};

			for (key, last_access) in cloned_map.iter() {
				if last_access.elapsed() > expiration_duration {
					let mut connection_pools_write = self.connection_pools.write().await;
					let mut access_map_write = self.access_map.write().await;

					connection_pools_write.remove(key);
					access_map_write.remove(key);
				}
			}
		}
	}
}
