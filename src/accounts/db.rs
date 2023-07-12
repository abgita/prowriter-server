use chrono::{Duration, Utc};
use log::LevelFilter;
use serde::{Serialize, Deserialize};
use sqlx::{Connection, ConnectOptions, Pool, Sqlite};
use sqlx::migrate::MigrateDatabase;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

use crate::accounts::AccountsError;
use crate::aclog;

const CREATE_USERS_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    pid TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    name TEXT NOT NULL,
    picture_url TEXT NOT NULL,
    given_name TEXT NOT NULL,
    family_name TEXT NOT NULL,
    role TEXT NOT NULL
)
"#;

const CREATE_REFRESH_TOKENS_TABLE_QUERY: &str = r#"
CREATE TABLE IF NOT EXISTS refresh_tokens (
    id INTEGER PRIMARY KEY,
    token TEXT UNIQUE NOT NULL,
    user_id INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
)
"#;

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
	pub id: i64,
	pub pid: String,
	pub email: String,
	pub password_hash: String,
	pub created_at: i64,
	pub name: String,
	pub picture_url: String,
	pub given_name: String,
	pub family_name: String,
}

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshToken {
	pub id: i64,
	pub token: String,
	pub user_id: i64,
	pub expires_at: i64,
}

pub async fn create_accounts_db(
	file_path: &str
) -> Result<(), AccountsError> {
	aclog!("Creating accounts database {}", file_path);

	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(true)
		.log_statements(LevelFilter::Debug)
		.connect().await
		.map_err(|e| AccountsError::SqlxError("Failed to create main database", e))?;

	let mut tx = conn.begin().await
		.map_err(|e| AccountsError::SqlxError("Failed to begin transaction", e))?;

	sqlx::query(CREATE_USERS_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to create users table", e))?;

	sqlx::query(CREATE_REFRESH_TOKENS_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to create refresh_tokens table", e))?;

	tx.commit().await
		.map_err(|e| AccountsError::SqlxError("Failed to commit transaction", e))?;

	conn.close().await
		.map_err(|e| AccountsError::SqlxErrorClosingConnection(e))?;

	aclog!("Tables created at {} successfully", file_path);

	Ok(())
}

#[inline]
pub async fn db_exists(db_path: &str) -> bool {
	return Sqlite::database_exists(db_path).await.unwrap_or(false);
}

pub async fn load_accounts_db(
	file_path: &str,
	max_connections: Option<u32>,
	min_connections: Option<u32>,
) -> Result<Pool<Sqlite>, AccountsError> {
	if !db_exists(file_path).await {
		create_accounts_db(file_path).await?;
	}

	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(false);
	let conn = conn.log_statements(LevelFilter::Debug).clone();

	let pool = SqlitePoolOptions::new()
		.max_connections(max_connections.unwrap_or(1))
		.min_connections(min_connections.unwrap_or(1))
		.max_lifetime(Some(Duration::hours(6).to_std().unwrap()))
		.idle_timeout(Some(Duration::minutes(30).to_std().unwrap()))
		.connect_with(conn)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to create accounts database pool", e))?;

	Ok(pool)
}

pub async fn does_user_exist(
    accounts_db: &Pool<Sqlite>,
    email: &str,
) -> Result<bool, AccountsError> {
	let row: (i64, ) = sqlx::query_as("SELECT COUNT(*) FROM users WHERE email = ?")
		.bind(email)
		.fetch_one(accounts_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to check if user exists", e))?;

	Ok(row.0 == 1)
}

pub async fn create_user(
	accounts_db: &Pool<Sqlite>,
	pid: &str,
	email: &str,
	password_hash: &str,
	name: &str,
	picture_url: &str,
	given_name: &str,
	family_name: &str,
) -> Result<i64, AccountsError> {
	let mut tx = accounts_db.begin().await
		.map_err(|e| AccountsError::SqlxError("Failed to begin transaction", e))?;

	let result = sqlx::query(
		r#"
        INSERT INTO users (
            pid,
            email,
            password_hash,
            created_at,
            name,
            picture_url,
            given_name,
            family_name,
            role
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
	)
		.bind(pid)
		.bind(email)
		.bind(password_hash)
		.bind(Utc::now().timestamp())
		.bind(name)
		.bind(picture_url)
		.bind(given_name)
		.bind(family_name)
		.bind("user")
		.execute(&mut tx)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to insert user", e))?;

	tx.commit().await
		.map_err(|e| AccountsError::SqlxError("Failed to commit transaction", e))?;

	let row_id = result.last_insert_rowid();

	Ok(row_id)
}

pub async fn get_user_by_user_pid(
	main_db: &Pool<Sqlite>,
	user_pid: &str,
) -> Result<Option<User>, AccountsError> {
	let row: Option<User> = sqlx::query_as("SELECT * FROM users WHERE pid = ?")
		.bind(user_pid)
		.fetch_optional(main_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to get user by pid", e))?;

	Ok(row)
}

pub async fn get_user_by_user_id(
	main_db: &Pool<Sqlite>,
	user_id: i64,
) -> Result<Option<User>, AccountsError> {
	let row: Option<User> = sqlx::query_as("SELECT * FROM users WHERE id = ?")
		.bind(user_id)
		.fetch_optional(main_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to get user by id", e))?;

	Ok(row)
}

pub async fn get_user_by_email(
	main_db: &Pool<Sqlite>,
	email: &str,
) -> Result<Option<User>, AccountsError> {
	let row: Option<User> = sqlx::query_as("SELECT * FROM users WHERE email = ?")
		.bind(email)
		.fetch_optional(main_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to get user by email", e))?;

	Ok(row)
}

pub async fn create_user_with_password(
	accounts_db: &Pool<Sqlite>,
	pid: &str,
	email: &str,
	password_hash: &str,
) -> Result<i64, AccountsError> {
	create_user(accounts_db, pid, email, &password_hash, "", "", "", "").await
}

pub async fn create_google_user(
	accounts_db: &Pool<Sqlite>,
	pid: &str,
	email: &str,
	name: &str,
	picture_url: &str,
	given_name: &str,
	family_name: &str,
) -> Result<(Option<User>, bool), AccountsError> {
	if does_user_exist(accounts_db, email).await? {
		return Ok((get_user_by_email(accounts_db, email).await?, false));
	}

	create_user(accounts_db, pid, email, "", name, picture_url, given_name, family_name).await?;

	Ok((get_user_by_email(accounts_db, email).await?, true))
}

pub async fn delete_user(
	accounts_db: &Pool<Sqlite>,
	user_id: i64,
) -> Result<u64, AccountsError> {
	// this should be done in a transaction, although it's not critical. we should clean up
	// refresh tokens frequently anyway
	let _ = _delete_all_refresh_tokens_for_user(accounts_db, user_id).await?;

	let result = sqlx::query("DELETE FROM users WHERE id = ?")
		.bind(user_id)
		.execute(accounts_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to delete user", e))?;

	Ok(result.rows_affected())
}

pub async fn store_refresh_token(
    accounts_db: &Pool<Sqlite>,
    token: &str,
    user_id: i64,
    expires_at: i64,
) -> Result<u64, AccountsError> {
	let result = sqlx::query(
		r#"
        INSERT INTO refresh_tokens (
            token,
            user_id,
            expires_at
        )
        VALUES (?, ?, ?)
        "#,
	)
		.bind(token)
		.bind(user_id)
		.bind(expires_at)
		.execute(accounts_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to store refresh token", e))?;

	Ok(result.rows_affected())
}

pub async fn get_refresh_token(
	accounts_db: &Pool<Sqlite>,
	token: &str,
) -> Result<Option<RefreshToken>, AccountsError> {
	let row: Option<RefreshToken> = sqlx::query_as(
		r#"
        SELECT * FROM refresh_tokens WHERE token = ?
        "#,
	)
		.bind(token)
		.fetch_optional(accounts_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to get refresh token", e))?;

	Ok(row)
}

pub async fn delete_refresh_token(
	accounts_db: &Pool<Sqlite>,
	token: &str,
) -> Result<u64, AccountsError> {
	let result = sqlx::query("DELETE FROM refresh_tokens WHERE token = ?")
		.bind(token)
		.execute(accounts_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to delete refresh token", e))?;

	Ok(result.rows_affected())
}

pub async fn _delete_all_refresh_tokens_for_user(
	accounts_db: &Pool<Sqlite>,
	user_id: i64,
) -> Result<u64, AccountsError> {
	let result = sqlx::query("DELETE FROM refresh_tokens WHERE user_id = ?")
		.bind(user_id)
		.execute(accounts_db)
		.await
		.map_err(|e| AccountsError::SqlxError("Failed to delete all refresh tokens for user", e))?;

	Ok(result.rows_affected())
}
