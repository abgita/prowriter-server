use std::sync::Arc;

use serde_json::json;
use warp::{ Rejection, Reply, reply};
use warp::http::StatusCode;

use crate::server::Error;
use crate::noctowl::doc_manager::{DocManager};

pub async fn create_doc_filter(
	doc_manager: Arc<DocManager>
) -> Result<impl Reply, Rejection> {
	match doc_manager.create_doc(true).await {
		Some(doc_id) => Ok(reply::with_status(
			reply::json(&json!({
                "docId": doc_id
            })),
			StatusCode::CREATED,
		)),
		None => Err(warp::reject::custom(Error {
			code: StatusCode::INTERNAL_SERVER_ERROR,
			messages: vec!["Failed to create document".to_string()],
		}))
	}
}
