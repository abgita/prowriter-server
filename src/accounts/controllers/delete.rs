use std::sync::Arc;

use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use tokio::sync::{RwLock};
use warp::{http::StatusCode, Rejection, reply, Reply};
use warp::reject::custom;

use crate::accounts::controllers::google::{RSAPublicKey, validate_google_id_token};
use crate::accounts::{db, CustomError};
use crate::accounts::jwt::AuthUser;

#[derive(Debug, Deserialize)]
pub struct DeleteRequest {
	pub password: Option<String>,
	#[serde(rename = "idToken")]
	pub id_token: Option<String>,
}

pub async fn delete(
	au: AuthUser,
	request: DeleteRequest,
	accounts_db: Pool<Sqlite>,
	public_keys: Arc<RwLock<Vec<RSAPublicKey>>>,
	google_client_id: String,
) -> Result<impl Reply, Rejection> {
	let user_pid = au.user_pid;

	let no_password = request.password.is_none() || request.password.clone().unwrap().is_empty();
	let no_id_token = request.id_token.is_none() || request.id_token.clone().unwrap().is_empty();

	if no_password && no_id_token {
		return Err(custom(CustomError::bad_request_single("Missing credentials")));
	}

	if request.id_token.is_some() {
		let id_token = request.id_token.unwrap();

		if validate_google_id_token(&id_token, &google_client_id, public_keys).await.is_none() {
			return Err(custom(CustomError {
				code: StatusCode::UNAUTHORIZED,
				messages: vec!["Invalid credentials".to_string()],
			}));
		}

		let db_user = db::get_user_by_user_pid(&accounts_db, &user_pid).await
			.map_err(|_| custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get user")))?;

		if let Some(user) = db_user {
			db::delete_user(&accounts_db, user.id).await.
				map_err(|_| custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete user")))?;

			return Ok(reply::with_status(
				reply::json(&()),
				StatusCode::OK,
			));
		}
	} else {
		let password = request.password.unwrap();

		let db_user = db::get_user_by_user_pid(&accounts_db, &user_pid).await
			.map_err(|_| custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to get user")))?;

		if let Some(user) = db_user {
			if bcrypt::verify(&password, &user.password_hash).unwrap() {
				db::delete_user(&accounts_db, user.id).await.
					map_err(|_| custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete user")))?;

				return Ok(reply::with_status(
					reply::json(&()),
					StatusCode::OK,
				));
			}
		}
	}

	Err(custom(CustomError::single(StatusCode::UNAUTHORIZED, "Invalid credentials")))
}
