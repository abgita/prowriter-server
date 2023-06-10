use chrono::Duration;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use sqlx::{Connection, ConnectOptions, Pool, Sqlite};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::fs;

use crate::common::utils;
use crate::{clog, nlog};
use crate::noctowl::db_project::create_project_db;
use crate::noctowl::db_utils::{db_exists, get_db_path, get_user_storage_path};
use crate::noctowl::NoctowlError;

const CREATE_PROJECTS_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS projects (
	project_id INTEGER PRIMARY KEY,
	project_pid TEXT UNIQUE NOT NULL,
	user_pid TEXT NOT NULL,
	name TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	last_accessed INTEGER NOT NULL
)
"#;

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
	pub project_id: i32,
	pub user_pid: String,
	pub project_pid: String,
	pub name: String,
	pub created_at: i64,
	pub last_accessed: i64,
}

async fn create_main_db(
	file_path: &str
) -> Result<(), NoctowlError> {
	nlog!("Creating main database {}", file_path);

	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(true)
		.log_statements(LevelFilter::Debug)
		.connect().await
		.map_err(|e| NoctowlError::SqlxError("Failed to create main database", e))?;

	// Begin a transaction
	let mut tx = conn.begin().await
		.map_err(|e| NoctowlError::SqlxError("Failed to begin transaction", e))?;


	sqlx::query(CREATE_PROJECTS_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to create projects table", e))?;

	// Create the index on user_pid
	sqlx::query(
		r#"CREATE INDEX IF NOT EXISTS user_pid_idx_projects ON projects (user_pid);"#
	)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to create index on user_pid", e))?;

	// Create the index on project_pid
	sqlx::query(
		r#"CREATE INDEX IF NOT EXISTS project_pid_idx_projects ON projects (project_pid);"#
	)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to create index on project_pid", e))?;

	// Commit the transaction
	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Failed to commit transaction", e))?;

	conn.close().await
		.map_err(|e| NoctowlError::SqlxErrorClosingConnection(e))?;

	nlog!("Tables created at {} successfully", file_path);

	Ok(())
}

pub async fn load_main_db(
	file_path: &str
) -> Result<Pool<Sqlite>, NoctowlError> {
	if !db_exists(file_path).await {
		create_main_db(file_path).await?;
	}

	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(false);
	let conn = conn.log_statements(LevelFilter::Debug).clone();

	let pool = SqlitePoolOptions::new()
		.max_connections(1)
		.min_connections(1)
		.max_lifetime(Some(Duration::hours(2).to_std().unwrap()))
		.idle_timeout(Some(Duration::minutes(30).to_std().unwrap()))
		.connect_with(conn)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to create main database pool", e))?;

	Ok(pool)
}

pub async fn does_project_exist(
	main_db: &Pool<Sqlite>,
	project_pid: &str,
) -> Result<bool, NoctowlError> {
	let row: (i64, ) = sqlx::query_as(
		r#"
        SELECT COUNT(*) FROM projects WHERE project_pid = ?
        "#,
	)
		.bind(project_pid)
		.fetch_one(main_db)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to check if project exists", e))?;

	Ok(row.0 == 1)
}

pub async fn add_project_if_not_exists(
	main_db: &Pool<Sqlite>,
	users_dir: &str,
	user_pid: &str,
	project_pid: &str,
	name: &str,
	created_at: i64,
	last_accessed: i64,
) -> Result<Option<i64>, NoctowlError> {
	let mut tx = main_db.begin().await
		.map_err(|e| NoctowlError::SqlxError("Failed to begin transaction", e))?;

	let row: (i64, ) = sqlx::query_as(
		r#"
        SELECT COUNT(*) FROM projects WHERE project_pid = ?
        "#,
	)
		.bind(project_pid)
		.fetch_one(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to check if project exists", e))?;

	if row.0 != 0 {
		tx.rollback().await
			.map_err(|e| NoctowlError::SqlxError("Failed to rollback transaction", e))?;

		return Ok(None);
	}

	clog!("Adding project: {}, {}, {}, {}, {}", user_pid, project_pid, name, created_at, last_accessed);

	let result = sqlx::query(
		r#"
        INSERT INTO projects (user_pid, project_pid, name, created_at, last_accessed)
        VALUES (?, ?, ?, ?, ?)
        "#,
	)
		.bind(user_pid)
		.bind(project_pid)
		.bind(name)
		.bind(created_at)
		.bind(last_accessed)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to insert project", e))?;

	let project_db_path = get_db_path(&users_dir, &user_pid, &project_pid);

	create_project_db(&project_db_path, &project_pid).await
		.map_err(|e| NoctowlError::Error("Failed to create project database", Box::new(e)))?;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Failed to commit transaction", e))?;

	let row_id = result.last_insert_rowid();

	Ok(Some(row_id))
}

pub async fn create_project(
	main_db: &Pool<Sqlite>,
	users_dir: &str,
	user_pid: &str,
	project_pid: &str,
	project_name: &str,
) -> Result<(), NoctowlError> {
	let user_storage_path = get_user_storage_path(&users_dir, &user_pid);

	utils::create_dirs_if_not_exists(&user_storage_path)
		.map_err(|e| NoctowlError::IoError(format!("Failed to create project directory: {}", e.to_string())))?;

	match add_project_if_not_exists(
		main_db,
		&users_dir,
		&user_pid,
		&project_pid,
		&project_name,
		utils::current_timestamp_secs() as i64,
		utils::current_timestamp_secs() as i64,
	).await {
		Ok(_) => {
			nlog!("Project inserted and project db created successfully");
		}

		Err(e) => {
			nlog!("Error inserting document: {}", e);

			let project_db_path = get_db_path(&users_dir, &user_pid, &project_pid);

			if db_exists(&project_db_path).await {
				match fs::remove_file(&project_db_path).await {
					Ok(_) => nlog!("Removed file: {}", &project_db_path),
					Err(e) => {
						nlog!("Error removing file: {}", &project_db_path);

						return Err(NoctowlError::IoError(format!("Failed to remove project database: {}", e.to_string())));
					}
				}
			} else {
				nlog!("Not removing database {}. Not created", &project_db_path);
			}

			return Err(NoctowlError::Error("Failed to create project", Box::new(e)));
		}
	}

	Ok(())
}

pub async fn get_project_by_pid(
	main_db: &Pool<Sqlite>,
	project_pid: &str,
) -> Result<Option<Project>, NoctowlError> {
	let row: Option<Project> = sqlx::query_as(
		r#"
        SELECT * FROM projects WHERE project_pid = ?
        "#,
	)
		.bind(project_pid)
		.fetch_optional(main_db)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to get project", e))?;

	Ok(row)
}

pub async fn get_projects_by_user_pid(
	main_db: &Pool<Sqlite>,
	user_pid: &str,
) -> Result<Vec<Project>, NoctowlError> {
	let rows: Vec<Project> = sqlx::query_as(
		r#"
        SELECT * FROM projects WHERE user_pid = ?
        "#,
	)
		.bind(user_pid)
		.fetch_all(main_db)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to get projects", e))?;

	Ok(rows)
}
