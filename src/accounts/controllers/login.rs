use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use warp::{http::StatusCode, Rejection, reply, Reply};
use warp::reject::custom;
use crate::accounts::controllers::refresh::generate_access_tokens;
use crate::accounts::db::User;
use crate::accounts::{AccountsError, db, CustomError};

use crate::{aclog};

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UserData {
    pub username: String,
    pub email: String,
    #[serde(rename = "avatarUrl")]
    pub avatar_url: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub user: UserData,
    #[serde(rename = "accessToken")]
    pub access_token: String,
    #[serde(rename = "tokenType")]
    pub token_type: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: i64,
    #[serde(rename = "refreshToken")]
    pub refresh_token: String,
}

pub async fn login(
    request: LoginRequest,
    accounts_db: Pool<Sqlite>,
) -> Result<impl Reply, Rejection> {
    if request.email.is_none() || request.email.clone().unwrap().is_empty() {
        return Err(custom(CustomError::bad_request_single("Email is required")));
    }

    if request.password.is_none() || request.password.clone().unwrap().is_empty() {
        return Err(custom(CustomError::bad_request_single("Password is required")));
    }

    let email = request.email.unwrap();
    let password = request.password.unwrap();

    let db_user = db::get_user_by_email(&accounts_db, &email).await.map_err(|e| {
        aclog!("Failed to authenticate user: {:?}", e);

        custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to authenticate user"))
    })?;

    if let Some(user) = db_user {
        if bcrypt::verify(&password, &user.password_hash).unwrap() {
            let response = login_user(&accounts_db, user.clone()).await.map_err(|e| {
                aclog!("Failed to authenticate user: {:?}", e);

                custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to authenticate user"))
            })?;

            return Ok(reply::with_status(
                reply::json(&response),
                StatusCode::OK,
            ));
        }
    }

    return Err(custom(CustomError::single(StatusCode::UNAUTHORIZED, "Invalid credentials")));
}

pub async fn login_user(
    accounts_db: &Pool<Sqlite>,
    user: User,
) -> Result<LoginResponse, AccountsError> {
    let tokens = generate_access_tokens(accounts_db, user.id, user.pid).await?;

    let user_data = UserData {
        username: user.name,
        email: user.email,
        avatar_url: user.picture_url,
    };

    Ok(LoginResponse {
        user: user_data,
        access_token: tokens.access_token,
        token_type: tokens.token_type,
        expires_at: tokens.expires_at,
        refresh_token: tokens.refresh_token,
    })
}
