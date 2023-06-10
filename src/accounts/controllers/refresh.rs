use std::env;

use chrono::{Duration, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use uuid::Uuid;
use warp::{http::StatusCode, Rejection, reply, Reply};
use warp::reject::custom;

use crate::{aclog};
use crate::accounts::{AccountsError, db, CustomError};
use crate::accounts::jwt::generate_jwt;

lazy_static! {
    static ref REFRESH_TOKEN_TTL_SECS: Duration = Duration::seconds(
        env::var("REFRESH_TOKEN_TTL_SECS").expect("REFRESH_TOKEN_TTL_SECS must be set").parse().unwrap()
    );
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    #[serde(rename = "refreshToken")]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccessToken {
    #[serde(rename = "accessToken")]
    pub access_token: String,
    #[serde(rename = "tokenType")]
    pub token_type: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: i64,
    #[serde(rename = "refreshToken")]
    pub refresh_token: String,
}

pub async fn refresh(
    request: RefreshRequest,
    accounts_db: Pool<Sqlite>,
) -> Result<impl Reply, Rejection> {
    if request.refresh_token.is_none() || request.refresh_token.clone().unwrap().is_empty() {
        return Err(custom(CustomError::bad_request_single("Refresh token is required")));
    }

    let refresh_token = request.refresh_token.unwrap();

    let db_refresh_token = db::get_refresh_token(&accounts_db, &refresh_token).await.map_err(|e| {
        aclog!("Error getting refresh token: {:?}", e);

        custom(CustomError::single(StatusCode::UNAUTHORIZED, "Invalid refresh token"))
    })?;

    if let Some(db_refresh_token) = db_refresh_token {
        let expires_at = db_refresh_token.expires_at;
        let user_id = db_refresh_token.user_id;

        if Utc::now().timestamp() < expires_at {
            // I'm not sure if we should fail here. At this point the refresh token is valid
            db::delete_refresh_token(&accounts_db, &refresh_token).await.map_err(|e| {
                aclog!("Error deleting refresh token: {:?}", e);

                custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, ""))
            })?;

            let user = db::get_user_by_user_id(&accounts_db, user_id).await.map_err(|e| {
                aclog!("Error getting user by user id: {:?}", e);

                custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, ""))
            })?;

            if user.is_none() {
                return Err(custom(CustomError::single(StatusCode::UNAUTHORIZED, "")));
            }

            let user = user.unwrap();

            let response = generate_access_tokens(&accounts_db, user.id, user.pid).await
              .map_err(|e| {
                aclog!("Error generating access tokens: {:?}", e);

                custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, ""))
            })?;

            return Ok(reply::with_status(
                reply::json(&response),
                StatusCode::OK,
            ));
        }
    }

    Err(custom(CustomError::single(StatusCode::UNAUTHORIZED, "Invalid refresh token")))
}

pub async fn generate_access_tokens(
    accounts_db: &Pool<Sqlite>,
    user_id: i64,
    user_pid: String,
) -> Result<AccessToken, AccountsError> {
    let refresh_token = Uuid::new_v4().to_string();

    let expires_at = (Utc::now() + *REFRESH_TOKEN_TTL_SECS).timestamp();

    db::store_refresh_token(&accounts_db, &refresh_token, user_id, expires_at).await?;

    let (claims, token) = generate_jwt(&user_pid);

    Ok(AccessToken {
        access_token: token,
        token_type: "Bearer".to_string(),
        expires_at: claims.exp,
        refresh_token: refresh_token.clone(),
    })
}
