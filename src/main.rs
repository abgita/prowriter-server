use std::net::Ipv4Addr;
use std::sync::Arc;

use dotenv::from_filename;
use warp::Filter;

use common::logging;
use crate::accounts::{Accounts, handle_rejection};
use crate::accounts::controllers::google::update_google_public_keys_task;

use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlError;

mod common;
mod api;
mod noctowl;
mod accounts;

fn load_env_variables() {
	if cfg!(debug_assertions) {
		from_filename(".env.development").ok();
	} else {
		// fetch secrets using google cloud secret manager
		// and setup the environment variables
	}
}

#[tokio::main]
async fn main() -> Result<(), NoctowlError> {
	logging::setup(false);
	load_env_variables();

	let noctowl = Noctowl::new(Some(".storage".to_string())).await?;
	let noctowl = Arc::new(noctowl);

	let accounts = Accounts::new(Some(".storage".to_string())).await.unwrap();

	tokio::spawn(update_google_public_keys_task(accounts.google_public_keys.clone()));

	let accounts_routes = accounts::routes::get_routes(accounts);
	let api_routes = api::routes::get_routes(&noctowl);

	if cfg!(debug_assertions) {
		let cors = warp::cors()
			.allow_methods(vec!["POST", "OPTIONS", "GET"])
			.allow_headers(vec!["Content-Type", "Authorization", "Accept"])
			.allow_any_origin();

		let routes = warp::path("v1")
			.and(accounts_routes.with(cors.clone())
				.or(api_routes.with(cors.clone())))
			.recover(handle_rejection);

		warp::serve(routes).run((Ipv4Addr::UNSPECIFIED, 3003)).await;
	} else {
		let routes = warp::path("v1")
			.and(accounts_routes
				.or(api_routes))
			.recover(handle_rejection);

		warp::serve(routes).run((Ipv4Addr::UNSPECIFIED, 3003)).await;
	}

	Ok(())
}
