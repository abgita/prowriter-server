use std::sync::{Arc};
use std::time::Duration;

use jsonwebtoken::{Algorithm, decode, DecodingKey, Validation};
use reject::custom;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{Pool, Sqlite};
use tokio::sync::RwLock;
use warp::{http::StatusCode, reject, Rejection, reply, Reply};
use crate::accounts::controllers::login::login_user;
use crate::accounts::{CustomError, db};
use crate::common::utils::get_new_user_pid;

use crate::{aclog};

#[derive(Debug, Deserialize)]
pub struct GoogleLoginRequest {
    #[serde(rename = "idToken")]
    pub id_token: Option<String>,
}

pub async fn google_login(
    request: GoogleLoginRequest,
    accounts_db: Pool<Sqlite>,
    public_keys: Arc<RwLock<Vec<RSAPublicKey>>>,
    google_client_id: String,
) -> Result<impl Reply, Rejection> {
    if request.id_token.is_none() || request.id_token.clone().unwrap().is_empty() {
        return Err(custom(CustomError::bad_request_single("idToken is required")));
    }

    let id_token = request.id_token.unwrap();

    if let Some(claims) = validate_google_id_token(&id_token, &google_client_id, public_keys).await {
        let email = claims.get("email").and_then(Value::as_str).unwrap_or("");
        let name = claims.get("name").and_then(Value::as_str).unwrap_or("");
        let picture_url = claims.get("picture").and_then(Value::as_str).unwrap_or("");
        let given_name = claims.get("given_name").and_then(Value::as_str).unwrap_or("");
        let family_name = claims.get("family_name").and_then(Value::as_str).unwrap_or("");

        let pid = get_new_user_pid();

        let (user, is_new) = db::create_google_user(
            &accounts_db,
            &pid,
            email,
            name,
            picture_url,
            given_name,
            family_name
          ).await.map_err(|e| {
              aclog!("Failed to authenticate user: {:?}", e);

              custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to authenticate user"))
          })?;


        if user.is_none() {
            return Err(custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to authenticate user")));
        }

        let user = user.unwrap();

        let response = login_user(&accounts_db, user.clone()).await
          .map_err(|e| {
              aclog!("Failed to authenticate user: {:?}", e);

              custom(CustomError::single(StatusCode::INTERNAL_SERVER_ERROR, "Failed to authenticate user"))
          })?;

        let status = if is_new {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        };

        return Ok(reply::with_status(
            reply::json(&response),
            status,
        ));
    }

    Err(custom(CustomError::single(StatusCode::UNAUTHORIZED, "Invalid idToken")))
}

pub async fn validate_google_id_token(id_token: &str, google_client_id: &str, public_keys: Arc<RwLock<Vec<RSAPublicKey>>>) -> Option<Value> {
    let mut validation = Validation::default();

    validation.algorithms = vec![Algorithm::RS256];
    validation.set_audience(&[google_client_id]);
    validation.set_issuer(&["accounts.google.com", "https://accounts.google.com"]);

    for key in public_keys.read().await.iter() {
        if let Ok(decoding_key) = DecodingKey::from_rsa_components(&key.modulus, &key.exponent) {
            match decode::<Value>(&id_token, &decoding_key, &validation) {
                Ok(decoded) => return Some(decoded.claims),
                Err(e) => println!("Decoding error: {:?}", e),
            }
        }
    }

    None
}

pub struct RSAPublicKey {
    pub modulus: String,
    pub exponent: String,
}

async fn fetch_google_public_keys() -> Result<Vec<RSAPublicKey>, reqwest::Error> {
    let url = "https://www.googleapis.com/oauth2/v3/certs";
    let resp: Value = Client::new().get(url).send().await?.json().await?;

    let public_keys = resp["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| RSAPublicKey {
            modulus: key["n"].as_str().unwrap().to_string(),
            exponent: key["e"].as_str().unwrap().to_string(),
        })
        .collect();

    Ok(public_keys)
}

pub async fn update_google_public_keys_task(public_keys: Arc<RwLock<Vec<RSAPublicKey>>>) {
    loop {
        match fetch_google_public_keys().await {
            Ok(new_public_keys) => {
                let mut write_lock = public_keys.write().await;
                *write_lock = new_public_keys;
            }

            Err(e) => eprintln!("Failed to update Google public keys: {:?}", e),
        }

        tokio::time::sleep(Duration::from_secs(60 * 60 * 2)).await;
    }
}
