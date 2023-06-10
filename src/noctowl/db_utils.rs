use std::path::Path;

use chrono::Duration;
use log::LevelFilter;
use sqlx::{ConnectOptions, Pool, Sqlite};
use sqlx::migrate::MigrateDatabase;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

use crate::noctowl::NoctowlError;

#[inline]
pub async fn db_exists(db_path: &str) -> bool {
	return Sqlite::database_exists(db_path).await.unwrap_or(false);
}

pub async fn create_short_lived_connection_pool(
	file_path: &str,
	min_connections: Option<u32>,
	max_connections: Option<u32>,
) -> Result<Pool<Sqlite>, NoctowlError> {
	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(false);
	let conn = conn.log_statements(LevelFilter::Debug).clone();

	let pool = SqlitePoolOptions::new()
		.min_connections(min_connections.unwrap_or(1))
		.max_connections(max_connections.unwrap_or(1))
		.max_lifetime(None)
		.idle_timeout(Some(Duration::minutes(30).to_std().unwrap()))
		.connect_with(conn)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to create short-lived database pool", e))?;

	Ok(pool)
}

#[inline]
pub fn get_user_storage_path(users_dir: &str, user_pid: &str) -> Box<Path> {
	Path::new(users_dir).join(user_pid).into_boxed_path()
}

#[inline]
pub fn get_db_path(users_dir: &str, user_pid: &str, db_name: &str) -> String {
	get_user_storage_path(users_dir, user_pid)
		.join(db_name)
		.with_extension("sqlite")
		.to_str().unwrap().to_string()
}

