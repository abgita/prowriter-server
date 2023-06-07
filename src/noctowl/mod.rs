mod storage;
mod fs_storage;

use std::collections::HashMap;
use std::fs;
use std::io::{Error, SeekFrom};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use uuid::Uuid;
use yrs::{Doc, merge_updates_v1, ReadTxn, StateVector, Transact, Update};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;

use crate::clog;
use crate::common::utils;

#[derive(Debug, Clone)]
pub struct Document {
	pub id: String,
	pub ydoc: Box<Doc>,
}

impl Document {
	fn new(name: &str) -> Self {
		Self {
			id: String::from(name),
			ydoc: Box::from(Doc::new()),
		}
	}

	pub fn get_doc_state(&self) -> Vec<u8> {
		self.ydoc.transact().encode_state_as_update_v1(&StateVector::default())
	}
}

pub struct DocManager {
	cache: HashMap<String, Arc<RwLock<Document>>>,
	docs_folder: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DocMetadata {
	#[serde(rename = "docId")]
	doc_id: String,
	#[serde(rename = "currentRevision")]
	current_revision: u64,
	#[serde(rename = "latestSnapshot")]
	latest_snapshot: u64,
}

struct Revision {
	timestamp: u64,
	data: Vec<u8>,
}

struct Snapshot {
	timestamp: u64,
	data: Vec<u8>,
}

const REV_NAME_ID: &str = "_rev";
const SNAP_NAME_ID: &str = "_snap";
const REVS_BEFORE_SNAP: usize = 80;

impl DocManager {
	pub fn new(docs_folder: &str) -> Self {
		if !Path::new(docs_folder).exists() {
			fs::create_dir(docs_folder).expect("Failed to create docs folder");
		}

		Self {
			cache: HashMap::new(),
			docs_folder: String::from(docs_folder),
		}
	}

	#[inline]
	pub fn is_doc_cached(&self, doc_id: &str) -> bool {
		self.cache.contains_key(doc_id)
	}

	#[inline]
	pub fn get_doc_from_cache(&self, doc_id: &str) -> &Arc<RwLock<Document>> {
		self.cache.get(doc_id).unwrap()
	}

	#[inline]
	pub fn cache_doc(&mut self, doc: Document) {
		self.cache.insert(doc.id.clone(), Arc::new(RwLock::new(doc)));
	}

	pub async fn get_snapshot_list(
		&self, doc_id: &str, amount: usize,
	) -> Result<Vec<(u64, u64)>, Box<Error>> {
		let mut snapshots: Vec<(u64, u64)> = Vec::new();

		let doc_metadata = self.load_doc_file(doc_id)?;
		let latest_snapshot = doc_metadata.latest_snapshot;

		if amount == 0 {
			return Ok(snapshots);
		}
		let amount = std::cmp::min(amount, latest_snapshot as usize + 1);

		// iterate from 0 to amount
		for i in 0..amount {
			let snapshot_id = latest_snapshot - (i as u64);
			let snapshot_path =
				format!("{}/{}/{}{}", self.docs_folder, doc_id, snapshot_id, SNAP_NAME_ID);

			let timestamp = self.read_snapshot_timestamp(&snapshot_path).await?;

			snapshots.push((snapshot_id, timestamp));
		}

		Ok(snapshots)
	}

	pub async fn load_doc_from_disk(
		&self,
		doc_id: &str,
		snapshot_id: i64,
	) -> Result<Document, Box<Error>> {
		clog!("Loading doc \"{}\" from disk", doc_id);
		let DocMetadata {
			doc_id: _,
			latest_snapshot,
			current_revision,
		} = self.load_doc_file(doc_id)?;

		let latest_snapshot = if snapshot_id == -1 {
			latest_snapshot
		} else {
			snapshot_id as u64
		};

		// get latest snapshot
		let snapshot_path =
			format!("{}/{}/{}{}", self.docs_folder, doc_id, latest_snapshot, SNAP_NAME_ID);

		let (_, snapshot) = self.read_snapshot(&snapshot_path).await?;

		let doc = Document::new(doc_id);
		doc.ydoc.get_or_insert_xml_text("root");

		self.apply_full_update(&doc.ydoc, snapshot);
		//self.log_doc_as_json(&doc.ydoc);

		println!("Loaded doc \"{}\" from snapshot", doc_id);
		let rev_file_name = format!("{}{}", latest_snapshot, REV_NAME_ID);
		let rev_file_path = format!("{}/{}/{}", self.docs_folder, doc_id, rev_file_name);

		if current_revision > 0 && Path::new(&rev_file_path).exists() {
			println!("Loading revs file: {}", rev_file_name);
			let (_, revs) = self.read_revs(&rev_file_path).await?;
			let updates: Vec<&[u8]> = revs.iter().map(AsRef::as_ref).collect();
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
						.apply_update(Update::decode_v1(&update).unwrap());
				}
			}
		}

		Ok(doc)
	}

	fn generate_doc_id(&self) -> String {
		Uuid::new_v4().to_string()
	}

	pub async fn create_doc(&mut self, create_root: bool) -> Option<String> {
		let doc_id: String = self.generate_doc_id();

		println!("Creating doc: {}", doc_id);

		let doc = Document::new(&doc_id);

		if create_root {
			doc.ydoc.get_or_insert_xml_text("root");
		}

		let dir = Path::new(&self.docs_folder).join(&doc_id);

		if !dir.exists() {
			fs::create_dir_all(&dir).map_err(|e| {
				eprintln!("Error creating directory: {}", e);
				e.kind()
			}).ok()?;
		}

		match self.save_snapshot(&doc_id, 0, &doc).await {
			Err(e) => {
				eprintln!("Error saving doc snapshot: {}", e);
				return None;
			}
			_ => (),
		};

		self.cache.insert(doc_id.clone(), Arc::new(RwLock::new(doc)));

		let metadata = DocMetadata {
			doc_id: doc_id.clone(),
			current_revision: 0,
			latest_snapshot: 0,
		};

		match self.save_doc_file(&doc_id, &metadata) {
			Err(e) => {
				eprintln!("Error saving doc file: {}", e);
				return None;
			}
			_ => (),
		};

		Some(doc_id)
	}

	pub async fn update_doc(
		&self,
		doc_id: &str,
		document: &mut Document,
		update: &[u8],
	) -> Result<(), Box<dyn std::error::Error>> {
		let doc = &document.ydoc;

		doc.transact_mut().apply_update(Update::decode_v1(update).unwrap());

		let mut doc_metadata = match self.load_doc_file(doc_id) {
			Ok(metadata) => metadata,
			Err(_) => return Err(Box::new(
				Error::new(
					ErrorKind::Other,
					format!("Failed to load doc metadata for doc {}", doc_id),
				)
			))
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

			self.save_snapshot(doc_id, next_snapshot, &document).await?;

			doc_metadata.latest_snapshot = next_snapshot;
		} else {
			let revision_id = format!("{}{}", latest_snapshot, REV_NAME_ID);
			let rev_file_path = format!("{}/{}/{}", self.docs_folder, doc_id, revision_id);

			clog!("Saving rev at: {}", rev_file_path);

			self.save_rev(&rev_file_path, update).await?;
		}

		self.save_doc_file(doc_id, &doc_metadata)?;

		Ok(())
	}

	fn apply_full_update(&self, doc: &Doc, message: Vec<u8>) {
		doc.transact_mut().apply_update(Update::decode_v1(&message).unwrap())
	}

	fn log_doc_as_json(&self, doc: &Doc) {
		log::info!("{}", doc.to_json(&doc.transact()));
	}

	async fn save_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64,
		doc: &Document,
	) -> io::Result<()> {
		let file_path = format!("{}/{}/{}{}", self.docs_folder, doc_id, snapshot_id, SNAP_NAME_ID);
		let mut file = File::create(&file_path).await?;

		let timestamp = utils::current_timestamp_secs();
		let timestamp_bytes: [u8; 8] = timestamp.to_be_bytes();
		file.write_all(&timestamp_bytes).await?;

		let doc_state: Vec<u8> = doc.get_doc_state();
		file.write_all(&doc_state).await?;

		Ok(())
	}

	async fn read_snapshot_timestamp(&self, file_path: &str) -> io::Result<u64> {
		let mut file = File::open(&file_path).await?;

		// Read the timestamp
		let mut timestamp_bytes = [0; 8];
		file.read_exact(&mut timestamp_bytes).await?;
		let timestamp = u64::from_be_bytes(timestamp_bytes);

		Ok(timestamp)
	}



	async fn read_snapshot(&self, file_path: &str) -> io::Result<(u64, Vec<u8>)> {
		let mut file = File::open(&file_path).await?;

		// Read the timestamp
		let mut timestamp_bytes = [0; 8];
		file.read_exact(&mut timestamp_bytes).await?;
		let timestamp = u64::from_be_bytes(timestamp_bytes);

		// Read the buffer data
		let mut buffer = Vec::new();
		file.read_to_end(&mut buffer).await?;

		Ok((timestamp, buffer))
	}

	async fn save_rev(&self, file_path: &str, rev_data: &[u8]) -> io::Result<()> {
		let mut file = OpenOptions::new()
			.write(true)
			.create(true)
			.append(true)
			.open(file_path)
			.await?;

		let timestamp = utils::current_timestamp_secs();
		let timestamp_bytes = timestamp.to_le_bytes();

		file.write_all(
			&(rev_data.len() as u32 + timestamp_bytes.len() as u32).to_le_bytes()
		).await?;
		file.write_all(&timestamp_bytes).await?;
		file.write_all(rev_data).await?;

		Ok(())
	}

	async fn read_revs(&self, file_path: &str) -> io::Result<(Vec<u64>, Vec<Vec<u8>>)> {
		let mut file = File::open(file_path).await?;
		let mut revs = Vec::new();
		let mut timestamps = Vec::new();
		let file_size = file.metadata().await?.len() as usize;
		let mut total_bytes_read = 0;

		//println!("File size: {}", file_size);

		while let Ok(n) = file.read(&mut [0; 1]).await {
			if n == 0 {
				break;
			}

			file.seek(SeekFrom::Current(-1)).await?;

			let mut rev_size_bytes = [0; 4];
			file.read_exact(&mut rev_size_bytes).await?;
			let rev_size = u32::from_le_bytes(rev_size_bytes) as u32;
			total_bytes_read += 4;

			let mut timestamp_bytes = [0; 8];
			file.read_exact(&mut timestamp_bytes).await?;
			let timestamp = u64::from_le_bytes(timestamp_bytes);
			total_bytes_read += 8;

			let rev_data_size = rev_size - 8;
			//println!("Rev data size: {}", rev_data_size);

			let mut rev_data = vec![0; rev_data_size as usize];
			file.read_exact(&mut rev_data).await?;

			total_bytes_read += rev_data_size;

			//println!("Total bytes read: {}", total_bytes_read);

			timestamps.push(timestamp);
			revs.push(rev_data);
		}

		Ok((timestamps, revs))
	}

	fn load_doc_file(&self, doc_id: &str) -> io::Result<DocMetadata> {
		let file_path = format!("{}/{}/doc.json", self.docs_folder, doc_id);

		log::info!("Loading doc metadata from: {}", file_path);

		let file = fs::File::open(file_path)?;
		let metadata: DocMetadata = serde_json::from_reader(file)?;

		Ok(metadata)
	}

	fn save_doc_file(&self, doc_id: &str, metadata: &DocMetadata) -> io::Result<()> {
		let file_path = format!("{}/{}/doc.json", self.docs_folder, doc_id);

		log::info!("Saving doc metadata at: {}. {:?}", file_path, metadata);

		let file = fs::File::create(file_path)?;
		serde_json::to_writer(file, metadata)?;

		Ok(())
	}
}

