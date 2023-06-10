use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use log::LevelFilter;
use serde::{Deserialize, Serialize};
use sqlx::{Connection, ConnectOptions, Pool, Sqlite, SqlitePool};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::common::utils;
use crate::nlog;
use crate::noctowl::db_document::create_doc_db_and_insert;
use crate::noctowl::db_main::does_project_exist;
use crate::noctowl::db_utils::{db_exists, create_short_lived_connection_pool, get_db_path};
use crate::noctowl::document::Document;
use crate::noctowl::NoctowlError;

const CREATE_FOLDERS_TABLE_QUERY: &str = r#"
CREATE TABLE folders (
	folder_id INTEGER PRIMARY KEY,
	project_pid TEXT NOT NULL,
	folder_name TEXT NOT NULL,
	folder_icon TEXT,
	parent_folder_id INTEGER,
	locked INTEGER NOT NULL,
	position INTEGER NOT NULL,
	FOREIGN KEY (parent_folder_id) REFERENCES folders(folder_id)
)
"#;

const CREATE_DOCS_TABLE_QUERY: &str = r#"
CREATE TABLE docs (
	doc_id INTEGER PRIMARY KEY,
	doc_pid TEXT UNIQUE NOT NULL,
	project_pid TEXT NOT NULL,
	folder_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	icon TEXT,
	locked INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	last_accessed INTEGER NOT NULL,
	position INTEGER NOT NULL,
	FOREIGN KEY(folder_id) REFERENCES folders(folder_id)
)
"#;

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderRow {
	pub folder_id: i32,
	pub project_pid: String,
	pub folder_name: String,
	pub folder_icon: Option<String>,
	pub parent_folder_id: Option<i32>,
	pub locked: i32,
	pub position: i32,
}

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocRow {
	pub doc_id: i32,
	pub doc_pid: String,
	pub project_pid: String,
	pub folder_id: i32,
	pub name: String,
	pub icon: Option<String>,
	pub locked: i32,
	pub created_at: i64,
	pub last_accessed: i64,
	pub position: i32,
}

const ROOT_FOLDER_ID: i32 = 2;

pub async fn create_project_db(
	file_path: &str,
	project_pid: &str,
) -> Result<(), NoctowlError> {
	nlog!("Creating project database {}", file_path);

	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(true)
		.log_statements(LevelFilter::Debug)
		.connect().await
		.map_err(|e| NoctowlError::SqlxError("Error creating project database connection", e))?;

	let mut tx = conn.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	sqlx::query(CREATE_FOLDERS_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating folders table", e))?;

	// Insert "trash" folder
	sqlx::query(
		r#"
        INSERT INTO folders (project_pid, folder_name, locked, position)
        VALUES (?, 'trash', 1, -1)
        "#,
	)
		.bind(project_pid)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting trash folder", e))?;

	// Insert "root" folder
	sqlx::query(
		r#"
        INSERT INTO folders (project_pid, folder_name, locked, position)
        VALUES (?, 'root', 1, -1)
        "#,
	)
		.bind(project_pid)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting root folder", e))?;

	sqlx::query(CREATE_DOCS_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating docs table", e))?;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	conn.close().await
		.map_err(|e| NoctowlError::SqlxErrorClosingConnection(e))?;

	nlog!("Tables created at {} successfully", file_path);

	Ok(())
}

pub async fn does_document_exist(
	project_db: &Pool<Sqlite>,
	doc_pid: &str,
) -> Result<bool, NoctowlError> {
	let row: (i64, ) = sqlx::query_as(
		r#"
        SELECT COUNT(*) FROM docs WHERE doc_pid = ?
        "#,
	)
		.bind(doc_pid)
		.fetch_one(project_db)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error checking if document exists", e))?;

	Ok(row.0 == 1)
}

pub async fn create_document(
	project_db_pool: &Pool<Sqlite>,
	users_dir: &str,
	doc_pid: &str,
	user_pid: &str,
	project_pid: &str,
	doc_name: &str,
	doc_icon: Option<String>,
	folder_id: Option<i32>,
) -> Result<Option<DocRow>, NoctowlError> {
	let doc_db_path = get_db_path(users_dir, user_pid, doc_pid);

	let new_doc_state = Document::new(doc_pid).get_doc_state();
	let snapshot_data = new_doc_state.as_slice();

	return match insert_doc_and_create_db(
		&project_db_pool,
		project_pid,
		&doc_db_path,
		doc_pid,
		doc_name,
		doc_icon,
		folder_id,
		utils::current_timestamp_secs() as i64,
		snapshot_data,
	).await {
		Ok(result) => {
			Ok(result)
		}
		Err(e) => {
			nlog!("Error inserting document: {}", e);

			if db_exists(&doc_db_path).await {
				fs::remove_file(&doc_db_path).await
					.map_err(|e| NoctowlError::IoError(format!("Error removing database: {}", e.to_string())))?;
			} else {
				nlog!("Not removing database {}. Not created", &doc_db_path);
			}

			Err(e)
		}
	};
}

pub async fn insert_doc_and_create_db(
	project_db: &Pool<Sqlite>,
	project_pid: &str,
	doc_db_file_path: &str,
	doc_pid: &str,
	doc_name: &str,
	doc_icon: Option<String>,
	folder_id: Option<i32>,
	current_timestamp: i64,
	snapshot_data: &[u8],
) -> Result<Option<DocRow>, NoctowlError> {
	let mut tx = project_db.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	let row: (i64, ) = sqlx::query_as(
		r#"
        SELECT COUNT(*) FROM docs WHERE doc_pid = ?
        "#,
	)
		.bind(doc_pid)
		.fetch_one(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error checking if document exists", e))?;

	if row.0 != 0 {
		tx.rollback().await
			.map_err(|e| NoctowlError::SqlxError("Error rolling back transaction", e))?;

		return Ok(None);
	}

	let folder_id = folder_id.unwrap_or(ROOT_FOLDER_ID);

	let max_position: i32 = sqlx::query_scalar("SELECT MAX(position) FROM docs WHERE folder_id = ?")
		.bind(folder_id)
		.fetch_one(&mut tx).await
		.map_err(|e| NoctowlError::SqlxError("Error getting max position", e))?;

	let doc_position = max_position + 1;

	let res = sqlx::query(
		r#"
        INSERT INTO docs (
        	doc_pid,
        	project_pid,
        	folder_id,
        	name,
        	icon,
        	locked,
        	created_at,
        	last_accessed,
        	position
        )
        VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
        "#,
	)
		.bind(doc_pid)
		.bind(project_pid)
		.bind(folder_id)
		.bind(doc_name)
		.bind(doc_icon.clone())
		.bind(current_timestamp)
		.bind(current_timestamp)
		.bind(doc_position)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting document", e))?;

	let doc_id = res.last_insert_rowid() as i32;

	match create_doc_db_and_insert(
		&doc_db_file_path,
		doc_pid,
		current_timestamp,
		snapshot_data,
	).await {
		Ok(_) => {}
		Err(error) => return match error {
			/*NoctowlError::SqlxErrorClosingConnection(e) => {
				// we might not want to rollback if we cannot close the connection. commented by now
			}*/

			_ => {
				tx.rollback().await
					.map_err(|e| NoctowlError::SqlxError("Error rolling back transaction", e))?;

				fs::remove_file(doc_db_file_path).await
					.map_err(|e| NoctowlError::IoError(format!("Error removing db file: {}", e)))?;

				fs::remove_file(doc_db_file_path.replace("sqlite", "sqlite-shm")).await
					.map_err(|e| NoctowlError::IoError(format!("Error removing sqlite-shm file: {}", e)))?;

				fs::remove_file(doc_db_file_path.replace("sqlite", "sqlite-wal")).await
					.map_err(|e| NoctowlError::IoError(format!("Error removing sqlite-wal file: {}", e)))?;

				Err(error)
			}
		}
	}

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok(Some(DocRow {
		doc_id,
		doc_pid: doc_pid.to_string(),
		project_pid: project_pid.to_string(),
		folder_id,
		name: doc_name.to_string(),
		icon: doc_icon.clone(),
		locked: 0,
		created_at: current_timestamp,
		last_accessed: current_timestamp,
		position: doc_position,
	}))
}

/**
 * Returns and existing project database connection from projects_db_pool or creates a new one
 */
pub async fn get_project_db_connection(
	main_db: &Pool<Sqlite>,
	connection_pools: &Arc<RwLock<HashMap<String, SqlitePool>>>,
	access_map: &Arc<RwLock<HashMap<String, Instant>>>,
	users_dir: &str,
	user_pid: &str,
	project_pid: &str,
) -> Result<Pool<Sqlite>, NoctowlError> {
	{
		access_map.write().await.insert(format!("{}:{}", user_pid, project_pid).to_string(), Instant::now());
	}

	let mut cache = connection_pools.write().await;

	match cache.entry(project_pid.to_string()) {
		Entry::Occupied(entry) => Ok(entry.get().clone()),
		Entry::Vacant(entry) => {
			let connection_pool = new_project_db_connection(
				&main_db,
				users_dir,
				user_pid,
				project_pid,
			).await?;

			entry.insert(connection_pool.clone());

			Ok(connection_pool)
		}
	}
}

async fn new_project_db_connection(
	main_db: &Pool<Sqlite>,
	users_dir: &str,
	user_pid: &str,
	project_pid: &str,
) -> Result<Pool<Sqlite>, NoctowlError> {
	// first we check if the project exists on the main database
	if !does_project_exist(&main_db, project_pid).await.unwrap_or(false) {
		return Err(NoctowlError::ProjectNotFound(project_pid.to_string()));
	}

	// now let's check if we have a local copy of the project's database
	let db_path = get_db_path(users_dir, user_pid, project_pid);

	if !db_exists(&db_path).await {
		// we need to fetch the database from AWS S3 before proceeding
	}

	let connection_pool = create_short_lived_connection_pool(
		&db_path,
		None,
		None,
	).await?;

	// load the database and get a connection pool
	Ok(connection_pool)
}

pub async fn get_project_content(
	project_db: &Pool<Sqlite>,
	project_pid: &str,
) -> Result<(Vec<FolderRow>, Vec<DocRow>), NoctowlError> {
	let mut tx = project_db.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	let folders: Vec<FolderRow> = sqlx::query_as(
		r#"
        SELECT * FROM folders WHERE project_pid = ?
        "#,
	)
		.bind(project_pid)
		.fetch_all(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error retrieving folders", e))?;

	let docs: Vec<DocRow> = sqlx::query_as(
		r#"
        SELECT * FROM docs WHERE project_pid = ?
        "#,
	)
		.bind(project_pid)
		.fetch_all(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error retrieving documents", e))?;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok((folders, docs))
}

pub async fn insert_folder(
	project_db: &Pool<Sqlite>,
	project_pid: &str,
	folder_name: &str,
	folder_icon: Option<String>,
	parent_folder_id: Option<i32>,
) -> Result<FolderRow, NoctowlError> {
	let mut tx = project_db.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	let max_position: i32 = sqlx::query_scalar("SELECT MAX(position) FROM folders WHERE folder_id IS ?")
		.bind(parent_folder_id.unwrap_or(ROOT_FOLDER_ID))
		.fetch_one(&mut tx).await
		.map_err(|e| NoctowlError::SqlxError("Error getting max position", e))?;

	let folder_position = max_position + 1;

	let res = sqlx::query(
		r#"
        INSERT INTO folders (project_pid, folder_name, folder_icon, locked, position, parent_folder_id)
        VALUES (?, ?, ?, 0, ?, ?)
        "#,
	)
		.bind(project_pid)
		.bind(folder_name)
		.bind(folder_icon.clone())
		.bind(folder_position)
		.bind(parent_folder_id)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting folder", e))?;

	let row_id = res.last_insert_rowid() as i32;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok(FolderRow {
		folder_id: row_id,
		project_pid: project_pid.to_string(),
		folder_name: folder_name.to_string(),
		folder_icon: folder_icon.clone(),
		locked: 0,
		position: folder_position,
		parent_folder_id,
	})
}

