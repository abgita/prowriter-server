use std::sync::Arc;

use warp::{Rejection, Reply, reply};
use warp::http::StatusCode;
use crate::api::routes::get_doc;

use crate::noctowl::doc_manager::{DocManager};
use crate::server::Error;

pub async fn get_doc_filter(
	doc_id: String,
	snapshot_id: i64,
	doc_manager: Arc<DocManager>,
) -> Result<impl Reply, Rejection> {
	if snapshot_id != -1 {
		let load_result = doc_manager.load_doc_from_disk(&doc_id, snapshot_id).await;

		return match load_result {
			Ok(doc) => {
				{
					let doc_state = {
						let doc = doc.read().await;

						doc.get_doc_state()
					};

					Ok(reply::with_status(
						reply::with_header(
							doc_state,
							"Content-Type",
							"application/octet-stream",
						),
						StatusCode::OK,
					))
				}
			}
			Err(_) => Err(warp::reject::custom(Error {
				code: StatusCode::NOT_FOUND,
				messages: vec!["Document not found".to_string()],
			}))
		};
	}

	match get_doc(doc_manager, &doc_id).await {
		Ok(doc) => {
			if doc.is_none() {
				return Err(warp::reject::custom(Error {
					code: StatusCode::NOT_FOUND,
					messages: vec!["Document not found".to_string()],
				}));
			}

			let doc = doc.unwrap();

			let doc_state = {
				let cached_doc = doc.read().await;

				cached_doc.get_doc_state()
			};

			Ok(reply::with_status(
				reply::with_header(
					doc_state,
					"Content-Type",
					"application/octet-stream",
				),
				StatusCode::OK,
			))
		}
		// todo: handle other kinds of errors and internal server errors
		Err(_) => Err(warp::reject::custom(Error {
			code: StatusCode::NOT_FOUND,
			messages: vec!["Document not found".to_string()],
		}))
	}
}
