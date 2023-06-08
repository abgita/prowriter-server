use std::sync::Arc;

use serde::Serialize;
use warp::{Rejection, Reply, reply};
use warp::http::StatusCode;

use crate::noctowl::doc_manager::{DocManager};
use crate::noctowl::storage::SnapshotInfo;
use crate::server::Error;

pub async fn get_snapshots_filter(
	doc_id: String,
	doc_manager: Arc<DocManager>,
) -> Result<impl Reply, Rejection> {
	match doc_manager.get_snapshot_list(&doc_id, 10).await {
		Ok(list) => {
			#[derive(Serialize)]
			struct SnapshotList {
				list: Vec<SnapshotInfo>,
			}

			Ok(reply::with_status(
				reply::json(&SnapshotList {
					list
				}),
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
