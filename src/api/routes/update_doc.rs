use std::sync::Arc;

use bytes::Bytes;
use warp::{Rejection, Reply, reply};
use warp::http::StatusCode;
use crate::api::routes::get_doc;

use crate::noctowl::doc_manager::{DocManager};
use crate::server::Error;

pub async fn update_doc_filter(
	doc_id: String,
	body: Bytes,
	doc_manager: Arc<DocManager>,
) -> Result<impl Reply, Rejection> {
	let update: Vec<u8> = body.iter().map(|b| *b).collect();

	return match get_doc(doc_manager.clone(), &doc_id).await {
		Ok(doc) => {
			return if let Some(doc) = doc {
				let mut doc = doc.write().await;

				match doc_manager.update_doc(&doc_id, &mut doc, &update).await {
					Ok(()) => Ok(reply::with_status(
						reply::json(&{}),
						StatusCode::OK,
					)),
					Err(e) => {
						return Err(warp::reject::custom(Error {
							code: StatusCode::INTERNAL_SERVER_ERROR,
							messages: vec![format!("Error {}, docId: {}", e, doc_id)],
						}));
					}
				}
			} else {
				Err(warp::reject::custom(Error {
					code: StatusCode::NOT_FOUND,
					messages: vec!["Document not found".to_string()],
				}))
			}
		}
		// todo: handle other kinds of errors and internal server errors
		Err(_) => Err(warp::reject::custom(Error {
			code: StatusCode::NOT_FOUND,
			messages: vec!["Document not found".to_string()],
		}))
	};
}
