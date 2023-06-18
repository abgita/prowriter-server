use std::convert::Infallible;
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;
use std::sync::{Arc};
use serde::{Serialize};
use sqlx::{Pool, Sqlite};
use tokio::sync::RwLock;
use warp::http::StatusCode;
use warp::{Rejection, reply, Reply};
use warp::reject::MissingHeader;
use crate::accounts::controllers::google::{RSAPublicKey};
use crate::accounts::db::load_accounts_db;
use crate::common::utils;

mod db;
pub mod jwt;
pub mod controllers;
pub mod routes;

#[derive(Clone)]
pub struct Accounts {
	pub db: Pool<Sqlite>,
	pub google_public_keys: Arc<RwLock<Vec<RSAPublicKey>>>,
	pub google_client_id: String,
}

impl Accounts {
	pub async fn new(storage_dir: Option<String>) -> Result<Self, AccountsError> {
		let google_client_id = env::var("GOOGLE_CLIENT_ID").expect("GOOGLE_CLIENT_ID must be set");

		let storage_dir = storage_dir.unwrap_or(".storage".to_string());

		let path = PathBuf::from(&storage_dir);
		let path = path.as_path();

		utils::create_dirs_if_not_exists(path).unwrap();

		// before creating a new database we must ensure we don't have a remote copy of the db
		// in the cloud. If so we should download it first
		let accounts_db = load_accounts_db(
			&format!("{}/accounts.sqlite", &storage_dir),
			// change these values from env variables
			Some(5),
			Some(1),
		).await
			.map_err(|e| AccountsError::Error(
				"Error loading accounts database",
				Box::new(e),
			))?;

		Ok(Accounts {
			db: accounts_db,
			google_public_keys: Arc::new(RwLock::new(Vec::<RSAPublicKey>::new())),
			google_client_id,
		})
	}

	pub async fn close(&self) {
		self.db.close().await;
	}
}

#[derive(Debug)]
pub struct CustomError {
	pub code: StatusCode,
	pub messages: Vec<String>,
}

impl warp::reject::Reject for CustomError {}

impl CustomError {
	pub fn single(code: StatusCode, message: &str) -> CustomError {
		CustomError {
			code,
			messages: vec![message.to_string()],
		}
	}

	pub fn bad_request_single(message: &str) -> CustomError {
		CustomError::single(StatusCode::BAD_REQUEST, message)
	}
}

#[derive(Serialize)]
pub struct ErrorMessage {
	pub errors: Vec<String>,
}

#[macro_export]
macro_rules! aclog {
	($($arg:tt)*) => (log::info!(target: "accounts", $($arg)*))
}

pub enum AccountsError {
	Error(&'static str, Box<dyn std::error::Error + Send + Sync>),
	IoError(String),
	SqlxError(&'static str, sqlx::Error),
	SqlxErrorClosingConnection(sqlx::Error),
}

impl Display for AccountsError {
	fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
		match self {
			AccountsError::Error(s, e) => write!(f, "{}: {}", s, e),
			AccountsError::IoError(s) => write!(f, "{}", s),
			AccountsError::SqlxError(s, e) => write!(f, "{}: {}", s, e),
			AccountsError::SqlxErrorClosingConnection(e) => write!(f, "Error closing connection: {}", e),
		}
	}
}

impl Debug for AccountsError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}

impl std::error::Error for AccountsError {}

pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
	let code: StatusCode;
	let mut error_messages: Vec<String> = vec![];

	if let Some(error) = err.find::<CustomError>() {
		code = error.code;
		error_messages = error.messages.clone();
	} else if let Some(_) = err.find::<MissingHeader>() {
		code = StatusCode::BAD_REQUEST;
	} else if let Some(_) = err.find::<warp::filters::body::BodyDeserializeError>() {
		code = StatusCode::BAD_REQUEST;
	} else {
		aclog!("Unhandled rejection: {:?}", err);

		code = StatusCode::INTERNAL_SERVER_ERROR;
	}

	let json = reply::json(&ErrorMessage {
		errors: error_messages,
	});

	Ok(reply::with_status(json, code))
}
