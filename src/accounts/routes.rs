use std::convert::Infallible;
use std::sync::Arc;

use sqlx::{Pool, Sqlite};
use tokio::sync::RwLock;
use warp::{Filter, Rejection, Reply};

use crate::accounts::controllers::{delete, google, login, refresh, signup};
use crate::accounts::controllers::google::{RSAPublicKey};
use crate::accounts::{Accounts, jwt};

fn with_string(
	value: String,
) -> impl Filter<Extract=(String, ), Error=Infallible> + Clone {
	warp::any().map(move || value.clone())
}

fn with_db_connection(
	connection: Pool<Sqlite>,
) -> impl Filter<Extract=(Pool<Sqlite>, ), Error=Infallible> + Clone {
	warp::any().map(move || connection.clone())
}

fn with_google_public_keys(
	public_keys: Arc<RwLock<Vec<RSAPublicKey>>>,
) -> impl Filter<Extract=(Arc<RwLock<Vec<RSAPublicKey>>>, ), Error=Infallible> + Clone {
	warp::any().map(move || public_keys.clone())
}

pub fn get_routes(
	accounts: Accounts
) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
	let db_connection = accounts.db;
	let public_keys = accounts.google_public_keys;
	let google_client_id = accounts.google_client_id;

	let signup_route = warp::post()
		.and(warp::path("signup"))
		.and(signup::validate_email_password())
		.and(with_db_connection(db_connection.clone()))
		.and_then(signup::signup);

	let login_route = warp::post()
		.and(warp::path("login"))
		.and(warp::body::content_length_limit(1024))
		.and(warp::body::form())
		.and(with_db_connection(db_connection.clone()))
		.and_then(login::login);

	let google_route = warp::post()
		.and(warp::path("google"))
		.and(warp::body::content_length_limit(8000))
		.and(warp::body::form())
		.and(with_db_connection(db_connection.clone()))
		.and(with_google_public_keys(public_keys.clone()))
		.and(with_string(google_client_id.clone()))
		.and_then(google::google_login);

	let refresh_route = warp::post()
		.and(warp::path("refresh"))
		.and(warp::body::json())
		.and(with_db_connection(db_connection.clone()))
		.and_then(refresh::refresh);

	let delete_route = warp::delete()
		.and(warp::path("delete"))
		.and(jwt::jwt_auth_filter())
		.and(warp::body::content_length_limit(8000))
		.and(warp::body::form())
		.and(with_db_connection(db_connection.clone()))
		.and(with_google_public_keys(public_keys.clone()))
		.and(with_string(google_client_id.clone()))
		.and_then(delete::delete);

	warp::any()
		.and(warp::path("accounts"))
			.and(signup_route
			.or(login_route)
			.or(google_route)
			.or(refresh_route)
			.or(delete_route))
}
