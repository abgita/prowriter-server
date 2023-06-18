use std::net::Ipv4Addr;
use std::sync::{Arc};

use dotenv::from_filename;
use tokio::sync::oneshot;
use warp::Filter;

use common::logging;
use crate::accounts::{Accounts, handle_rejection};
use crate::accounts::controllers::google::update_google_public_keys_task;

use crate::noctowl::lib::{clean_up_stale_connections_task, Noctowl};
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
	tokio::spawn(clean_up_stale_connections_task(noctowl.clone()));

	let accounts_routes = accounts::routes::get_routes(accounts.clone());
	let api_routes = api::routes::get_routes(&noctowl);

	// Create a broadcast channel for the shutdown signal
	let (tx, rx) = oneshot::channel();

	// Capture the Ctrl+C signal to trigger a shutdown
	let shutdown_signal = tokio::spawn(async {
		tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C signal");
		tx.send(()).expect("Failed to send shutdown signal");
	});

	// Run the server with the provided routes and a closure that captures the shutdown signal
	let server_fut = if cfg!(debug_assertions) {
		let cors = warp::cors()
			.allow_methods(vec!["POST", "OPTIONS", "GET"])
			.allow_headers(vec!["Content-Type", "Authorization", "Accept"])
			.allow_any_origin();

		let routes = warp::path("v1")
			.and(accounts_routes.with(cors.clone())
				.or(api_routes.with(cors.clone())))
			.recover(handle_rejection);

		let (_, server) = warp::serve(routes)
			.bind_with_graceful_shutdown((Ipv4Addr::UNSPECIFIED, 3003), async {
			rx.await.ok();

			println!("Server shutdown gracefully");
		});

		tokio::task::spawn(server)
	} else {
		let routes = warp::path("v1")
			.and(accounts_routes
				.or(api_routes))
			.recover(handle_rejection);

		let (_, server) = warp::serve(routes)
			.bind_with_graceful_shutdown((Ipv4Addr::UNSPECIFIED, 3003), async {
			rx.await.ok();

			println!("Server shutdown gracefully");
		});

		tokio::task::spawn(server)
	};

	// Run the server and the shutdown signal listener concurrently
	let _ = tokio::select! {
    _ = server_fut => println!("Server exited unexpectedly"),
    _ = shutdown_signal => {
            if let Err(e) = close_db(noctowl.clone(), accounts).await {
                println!("Error closing the databases: {:?}", e);
            }
        }
    };

	Ok(())
}

async fn close_db(
	noctowl: Arc<Noctowl>,
	accounts: Accounts
) -> Result<(), NoctowlError> {
	noctowl.clean_up().await;
	accounts.close().await;

	Ok(())
}
