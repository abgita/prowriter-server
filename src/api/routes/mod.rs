pub mod update_doc;
pub mod get_snapshots;
pub mod get_doc;
pub mod create_doc;

use std::convert::Infallible;
use std::error::Error;
use std::sync::Arc;

use tokio::sync::RwLock;
use warp::Filter;

use crate::noctowl::doc_manager::{DocManager, Document};

async fn get_doc(
	doc_manager: Arc<DocManager>,
	doc_id: &str,
) -> Result<Option<Arc<RwLock<Document>>>, Box<dyn Error + Send + Sync>> {
	if doc_manager.is_doc_cached(&doc_id).await {
		return Ok(doc_manager.get_doc_from_cache(&doc_id).await);
	}

	return match doc_manager.load_doc_from_disk(&doc_id, -1).await {
		Ok(doc) => {
			doc_manager.cache_doc(doc.clone()).await;

			return Ok(Some(doc));
		}
		Err(m) => Err(m)
	};
}
