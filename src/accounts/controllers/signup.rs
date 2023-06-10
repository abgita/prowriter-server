use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use validator::validate_email;
use warp::{Filter, http::StatusCode, Rejection, Reply};
use warp::reject::custom;

use crate::{aclog};
use crate::accounts::{db, CustomError};
use crate::common::utils::get_new_user_pid;

#[derive(Debug, Deserialize)]
pub struct EmailPasswordRequest {
    pub email: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
}

fn get_email_validation_errors(email: Option<&str>) -> Option<String> {
    match email {
        None => Some("Email is required".to_string()),
        Some(email) => {
            if email.len() >= 255 {
                Some("Email must be less than 255 characters".to_string())
            } else {
                if !validate_email(email) {
                    Some("Invalid email".to_string())
                } else {
                    None
                }
            }
        }
    }
}

fn get_password_validation_errors(password: Option<&str>) -> Option<String> {
    match password {
        None => Some("Password is required".to_string()),
        Some(password) => {
            if password.len() < 8 {
                Some("Password must be at least 8 characters".to_string())
            } else {
                None
            }
        }
    }
}

fn get_validation_errors(email: Option<&str>, password: Option<&str>) -> Vec<String> {
    let email_error = get_email_validation_errors(email);
    let password_error = get_password_validation_errors(password);
    let mut validation_errors = Vec::new();

    if let Some(error) = email_error {
        validation_errors.push(error);
    }

    if let Some(error) = password_error {
        validation_errors.push(error);
    }

    validation_errors
}

pub fn validate_email_password() -> impl Filter<Extract=(SignupRequest, ), Error=Rejection> + Copy {
    warp::body::content_length_limit(1024)
        .and(warp::body::form())
        .and_then(|request: EmailPasswordRequest| async move {
            let validation_errors = get_validation_errors(request.email.as_deref(), request.password.as_deref());

            if validation_errors.is_empty() {
                Ok(SignupRequest {
                    email: request.email.unwrap(),
                    password: request.password.unwrap(),
                })
            } else {
                Err(custom(CustomError {
                    code: StatusCode::BAD_REQUEST,
                    messages: validation_errors,
                }))
            }
        })
}

pub async fn signup(
    request: SignupRequest,
    accounts_db: Pool<Sqlite>,
) -> Result<impl Reply, Rejection> {
    let SignupRequest {
        email,
        password,
    } = request;

    // this should be a single transaction, refactor later.
    //let conn = conn.lock().await;

    let exists = db::does_user_exist(&accounts_db, &email).await.map_err(|err| {
        aclog!("Failed to check if user exists {:?}", err);

        custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to check if user exists"))
    })?;

    if exists {
        return Err(custom(CustomError::single(StatusCode::CONFLICT, "Email already exists")));
    }

    let password_hash = bcrypt::hash(&password, bcrypt::DEFAULT_COST).unwrap();
    let pid = get_new_user_pid();

    db::create_user_with_password(&accounts_db, &pid, &email, &password_hash).await.map_err(|err| {
        aclog!("Failed to create user {:?}", err);

        custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create user"))
    })?;

    Ok(StatusCode::CREATED)
}
