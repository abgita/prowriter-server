use std::collections::HashMap;
use std::io::{Error};
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use yrs::{Doc, merge_updates_v1, ReadTxn, StateVector, Transact, Update};
use yrs::updates::decoder::Decode;

use crate::clog;
use crate::common::utils;
use crate::noctowl::fs_storage::FileSystemStorage;
use crate::noctowl::storage::{DocMetadata, Revision, Snapshot, SnapshotInfo};

#[derive(Debug, Clone)]
pub struct Document {
	pub id: String,
	pub ydoc: Box<Doc>,
	pub lock: Arc<Mutex<()>>
}

impl Document {
	fn new(name: &str) -> Self {
		Self {
			id: String::from(name),
			ydoc: Box::from(Doc::new()),
			lock: Arc::new(Mutex::new(()))
		}
	}

	pub fn get_doc_state(&self) -> Vec<u8> {
		self.ydoc.transact().encode_state_as_update_v1(&StateVector::default())
	}
}

pub struct DocManager {
	pub cache: RwLock<HashMap<String, Arc<RwLock<Document>>>>
}

impl DocManager {
	pub fn new() -> Self {
		Self {
			cache: RwLock::new(HashMap::new()),
		}
	}
}

const REVS_BEFORE_SNAP: usize = 80;

impl DocManager {
	#[inline]
	pub async fn is_doc_cached(&self, doc_id: &str) -> bool {
		self.cache.read().await.contains_key(doc_id)
	}

	#[inline]
	pub async fn get_doc_from_cache(&self, doc_id: &str) -> Option<Arc<RwLock<Document>>> {
		{
			let cache = self.cache.read().await;

			match cache.get(doc_id) {
				Some(doc) => Some(doc.clone()),
				None => None,
			}
		}
	}

	#[inline]
	pub async fn cache_doc(&self, doc: Arc<RwLock<Document>>) {
		let mut cache = self.cache.write().await;

		let id = doc.read().await.id.clone();

		cache.insert(id, doc);
	}

	pub async fn get_snapshot_list(
		&self, doc_id: &str, amount: usize,
	) -> Result<Vec<SnapshotInfo>, Box<Error>> {
		match FileSystemStorage::new("docs").get_latest_snapshots(doc_id, amount).await {
			Ok(snapshots) => Ok(snapshots),
			Err(e) => {
				log::error!("{}", e);

				return Err(Box::new(Error::new(
					std::io::ErrorKind::NotFound,
					"Document not found",
				)))
			}
		}
	}

	pub async fn load_doc_from_disk(
		&self,
		doc_id: &str,
		snapshot_id: i64,
	) -> Result<Arc<RwLock<Document>>, Box<Error>> {
		clog!("Loading doc \"{}\" from disk", doc_id);

		let fs = FileSystemStorage::new("docs");

		let DocMetadata {
			doc_id: _,
			latest_snapshot,
			current_revision,
		} = match fs.load_metadata(doc_id).await {
			Ok(metadata) => metadata,
			Err(e) => {
				log::error!("{}", e);

				return Err(Box::new(Error::new(
					std::io::ErrorKind::NotFound,
					"Document not found",
				)))
			}
		};

		let latest_snapshot = if snapshot_id == -1 {
			latest_snapshot
		} else {
			snapshot_id as u64
		};

		let snapshot = match fs.load_snapshot(doc_id, latest_snapshot)
			.await {
			Ok(snapshot) => snapshot,
			Err(e) => {
				log::error!("{}", e);

				return Err(Box::new(Error::new(
					std::io::ErrorKind::NotFound,
					"Snapshot not found",
				)))
			}
		};

		let doc = Document::new(doc_id);
		doc.ydoc.get_or_insert_xml_text("root");

		self.apply_full_update(&doc.ydoc, snapshot.data);

		println!("Loaded doc \"{}\" from snapshot", doc_id);

		if current_revision > 0 {
			let revs = match fs.load_revisions(doc_id, latest_snapshot)
				.await {
				Ok(revs) => revs,
				Err(e) => {
					log::error!("{}", e);

					return Err(Box::new(Error::new(
						std::io::ErrorKind::NotFound,
						"Revisions not found",
					)))
				}
			};

			let updates: Vec<&[u8]> = revs.iter().map(|rev| rev.data.as_ref()).collect();

			println!("Revs loaded. Applying updates...");

			// we should measure which is faster later
			let should_merge_updates = true;

			if should_merge_updates {
				let merged_updates = merge_updates_v1(&updates).unwrap();

				doc.ydoc
					.transact_mut()
					.apply_update(Update::decode_v1(&merged_updates).unwrap());
			} else {
				for update in revs {
					doc.ydoc
						.transact_mut()
						.apply_update(Update::decode_v1(&update.data).unwrap());
				}
			}
		}

		let doc_arc = Arc::new(RwLock::new(doc));

		Ok(doc_arc)
	}

	fn generate_doc_id(&self) -> String {
		Uuid::new_v4().to_string()
	}

	pub async fn create_doc(&self, create_root: bool) -> Option<String> {
		let doc_id: String = self.generate_doc_id();

		println!("Creating doc: {}", doc_id);

		let doc = Document::new(&doc_id);

		if create_root {
			doc.ydoc.get_or_insert_xml_text("root");
		}

		let snapshot = Snapshot {
			timestamp: utils::current_timestamp_secs(),
			data: doc.get_doc_state(),
		};

		let fs = FileSystemStorage::new("docs");

		let doc_entry_id = match fs.create_doc(&doc_id, &snapshot).await {
			Ok(id) => id,
			Err(e) => {
				log::error!("{}", e);

				return None;
			}
		};

		{
			let mut cache = self.cache.write().await;

			cache.insert(doc_id.clone(), Arc::new(RwLock::new(doc)));
		}

		let metadata = DocMetadata {
			doc_id: doc_id.clone(),
			current_revision: 0,
			latest_snapshot: 0,
		};

		match fs.save_metadata(&doc_id, &metadata).await {
			Ok(_) => (),
			Err(e) => {
				log::error!("{}", e);
			}
		};

		Some(doc_entry_id)
	}

	pub async fn update_doc(
		&self,
		doc_id: &str,
		document: &mut Document,
		update: &[u8],
	) -> Result<(), Box<dyn std::error::Error>> {
		let _guard = document.lock.lock().await;

		let fs = FileSystemStorage::new("docs");

		let mut doc_metadata = match fs.load_metadata(doc_id).await {
			Ok(metadata) => metadata,
			Err(e) => {
				log::error!("{}", e);

				return Err(Box::new(Error::new(
					std::io::ErrorKind::NotFound,
					"Document not found",
				)))
			}
		};

		let current_revision = doc_metadata.current_revision;
		let latest_snapshot = doc_metadata.latest_snapshot;

		let next_revision = current_revision + 1;
		let should_save_snapshot = next_revision % REVS_BEFORE_SNAP as u64 == 0;

		println!("Saving {} for doc {}", if should_save_snapshot {
			"snapshot"
		} else {
			"revision"
		}, doc_id);

		doc_metadata.current_revision = next_revision;

		if should_save_snapshot {
			let next_snapshot = latest_snapshot + 1;

			let new_snapshot = Snapshot {
				timestamp: utils::current_timestamp_secs(),
				data: document.get_doc_state(),
			};

			match fs.save_snapshot(doc_id, next_snapshot, &new_snapshot).await {
				Ok(_) => (),
				Err(e) => {
					log::error!("{}", e);

					return Err(Box::new(Error::new(
						std::io::ErrorKind::NotFound,
						"Snapshot not found",
					)))
				}
			};

			doc_metadata.latest_snapshot = next_snapshot;
		} else {
			let revision = Revision {
				timestamp: utils::current_timestamp_secs(),
				data: update.to_vec(),
			};

			match fs.save_revision(&doc_id, latest_snapshot, &revision).await {
				Ok(_) => (),
				Err(e) => {
					log::error!("{}", e);

					return Err(Box::new(Error::new(
						std::io::ErrorKind::NotFound,
						"Revision not found",
					)))
				}
			};
		}

		match fs.save_metadata(doc_id, &doc_metadata).await {
			Ok(_) => (),
			Err(e) => {
				log::error!("{}", e);

				return Err(Box::new(Error::new(
					std::io::ErrorKind::NotFound,
					"Document not found",
				)))
			}
		};

		let doc = &document.ydoc;
		doc.transact_mut().apply_update(Update::decode_v1(update).unwrap());

		Ok(())
	}

	fn apply_full_update(&self, doc: &Doc, message: Vec<u8>) {
		doc.transact_mut().apply_update(Update::decode_v1(&message).unwrap())
	}
}
