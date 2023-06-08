use std::net::Ipv4Addr;
use std::sync::Arc;

use dotenv::from_filename;
use warp::Filter;

use common::logging;
use noctowl::doc_manager::DocManager;

use crate::api::server;

mod common;
mod api;
mod noctowl;

fn load_env_variables() {
	if cfg!(debug_assertions) {
		from_filename(".env.development").ok();
	} else {
		// fetch secrets using google cloud secret manager
		// and setup the environment variables
	}
}

#[tokio::main]
async fn main() {
	logging::setup(false);
	load_env_variables();

	let doc_manager = Arc::new(DocManager::new());

	if cfg!(debug_assertions) {
		let cors = warp::cors()
				.allow_methods(vec!["POST", "OPTIONS", "GET"])
				.allow_headers(vec!["Content-Type", "Authorization", "Accept"])
				.allow_any_origin();

		let routes = server::create_routes(doc_manager).with(cors);

		warp::serve(routes).run((Ipv4Addr::UNSPECIFIED, 3003)).await;
	} else {
		let routes = server::create_routes(doc_manager);
		warp::serve(routes).run((Ipv4Addr::UNSPECIFIED, 3003)).await;
	}
}
