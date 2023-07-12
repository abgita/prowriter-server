use std::env;

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, DecodingKey, encode, EncodingKey, Header, Validation};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};
use warp::http::StatusCode;

use crate::accounts::CustomError;

lazy_static! {
    static ref JWT_SECRET: String = env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    static ref JWT_TTL_SECS: i64 = env::var("JWT_TTL_SECS").expect("JWT_TTL_SECS must be set").parse().unwrap();
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
	pub sub: String,
	pub iat: i64,
	pub exp: i64,
	pub tdid: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthUser {
	pub user_pid: String,
	pub temp_device_id: i32,
}

pub fn generate_jwt(
	pid: &str
) -> (Claims, String) {
	let header = Header::default();

	let payload = Claims {
		sub: pid.to_string(),
		iat: Utc::now().timestamp(),
		exp: (Utc::now() + Duration::seconds(*JWT_TTL_SECS)).timestamp(),
		tdid: Utc::now().timestamp() as i32,
	};

	let encoding_key = EncodingKey::from_secret((*JWT_SECRET).as_ref());

	(payload.clone(), encode(&header, &payload, &encoding_key).unwrap())
}

pub fn jwt_auth_filter() -> impl Filter<Extract=(AuthUser, ), Error=Rejection> + Clone {
	warp::header("Authorization")
		.map(move |auth_header: String| (auth_header, (*JWT_SECRET).clone()))
		.and_then(|(auth_header, jwt_secret): (String, String)| async move {
			let parts: Vec<&str> = auth_header.split(' ').collect();

			if parts.len() != 2 || parts[0].to_lowercase() != "bearer" {
				return Err(warp::reject::custom(CustomError {
					code: StatusCode::BAD_REQUEST,
					messages: vec!["Invalid auth header".to_string()],
				}));
			}

			let token = parts[1];
			let decoding_key = DecodingKey::from_secret(jwt_secret.as_ref());
			let validation = Validation::default();

			match decode::<Claims>(&token, &decoding_key, &validation) {
				Ok(decoded) => Ok(AuthUser {
					user_pid: decoded.claims.sub,
					temp_device_id: decoded.claims.tdid,
				}),
				Err(_) => Err(warp::reject::custom(CustomError {
					code: StatusCode::UNAUTHORIZED,
					messages: vec!["Invalid JWT".to_string()],
				})),
			}
		})
}
