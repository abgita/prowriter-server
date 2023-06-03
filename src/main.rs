use std::net::Ipv4Addr;
use std::sync::Arc;

use dotenv::from_filename;
use tokio::sync::RwLock;

use common::logging;
use noctowl::DocManager;

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

	let doc_manager = Arc::new(RwLock::new(DocManager::new("docs")));

	let routes = server::create_routes(doc_manager);
	warp::serve(routes).run((Ipv4Addr::UNSPECIFIED, 3003)).await;
}
