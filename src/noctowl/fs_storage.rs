use std::{fs};
use std::io::{SeekFrom};
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::clog;
use crate::noctowl::storage::{
	Snapshot,
	SnapshotInfo,
	StorageError,
	DocMetadata,
	Revision
};

pub struct FileSystemStorage {
	docs_folder: String,
}

const REV_NAME_ID: &str = "_rev";
const SNAP_NAME_ID: &str = "_snap";

impl FileSystemStorage {
	pub fn new(docs_folder: &str) -> Self {
		Self {
			docs_folder: docs_folder.to_string(),
		}
	}
}

impl FileSystemStorage {
	pub(crate) async fn create_doc(
		&self,
		doc_id: &str,
		snapshot: &Snapshot,
	) -> Result<String, StorageError> {
		let dir = Path::new(&self.docs_folder).join(&doc_id);

		if !dir.exists() {
			match fs::create_dir_all(&dir).map_err(|e| {
				eprintln!("Error creating directory: {}", e);
				e.kind()
			}) {
				Ok(_) => (),
				Err(e) => {
					return Err(StorageError::SaveError(format!(
						"Failed to create directory: {}",
						e
					)))
				}
			}
		}

		self.save_snapshot(&doc_id, 0, &snapshot).await?;

		let metadata = DocMetadata {
			doc_id: doc_id.to_string(),
			current_revision: 0,
			latest_snapshot: 0,
		};

		self.save_metadata(&doc_id, &metadata).await?;

		Ok(doc_id.to_string())
	}

	async fn load_doc(
		&self,
		doc_id: &str,
		snapshot_id: i64
	) -> Result<(Snapshot, Option<Vec<Revision>>), StorageError> {
		clog!("Loading doc \"{}\" from disk", doc_id);

		let DocMetadata {
			doc_id: _,
			latest_snapshot,
			current_revision,
		} = self.load_metadata(doc_id).await?;

		let latest_snapshot = if snapshot_id == -1 {
			latest_snapshot
		} else {
			snapshot_id as u64
		};

		let snapshot = match self.load_snapshot(&doc_id, latest_snapshot).await {
			Ok(s) => s,
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to load snapshot: {}",
					e
				)))
			}
		};

		let rev_file_name = format!("{}{}", latest_snapshot, REV_NAME_ID);
		let rev_file_path = format!("{}/{}/{}", self.docs_folder, doc_id, rev_file_name);

		if current_revision > 0 && Path::new(&rev_file_path).exists() {
			println!("Loading revs file: {}", rev_file_name);

			let revs = match self.load_revisions(&doc_id, latest_snapshot).await {
				Ok(r) => r,
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to load revisions: {}",
						e
					)))
				}
			};

			return Ok((snapshot, Some(revs)));
		}

		Ok((snapshot, None))
	}

	pub(crate) async fn load_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64
	) -> Result<Snapshot, StorageError> {
		let file_path =
			format!("{}/{}/{}{}", self.docs_folder, doc_id, snapshot_id, SNAP_NAME_ID);

		let mut file = match File::open(&file_path).await {
			Ok(f) => f,
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to load snapshot: {}",
					e
				)))
			}
		};

		// Read the timestamp
		let mut timestamp_bytes = [0; 8];

		let timestamp = match file.read_exact(&mut timestamp_bytes).await {
			Ok(_) => u64::from_be_bytes(timestamp_bytes),
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to read timestamp: {}",
					e
				)))
			}
		};

		// Read the buffer data
		let mut buffer = Vec::new();

		match file.read_to_end(&mut buffer).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to read snapshot data: {}",
					e
				)))
			}
		};

		Ok(Snapshot {
			timestamp,
			data: buffer
		})
	}

	pub(crate) async fn get_latest_snapshots(
		&self,
		doc_id: &str,
		amount: usize
	) -> Result<Vec<SnapshotInfo>, StorageError> {
		let mut snapshots: Vec<SnapshotInfo> = Vec::new();

		let doc_metadata = match self.load_metadata(doc_id).await {
			Ok(m) => m,
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to load metadata file: {}",
					e
				)))
			}
		};

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

			let mut file = match File::open(&snapshot_path).await {
				Ok(f) => f,
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to load snapshot: {}",
						e
					)))
				}
			};

			// Read the timestamp
			let mut timestamp_bytes = [0; 8];
			let timestamp = match file.read_exact(&mut timestamp_bytes).await {
				Ok(_) => u64::from_be_bytes(timestamp_bytes),
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to read timestamp: {}",
						e
					)))
				}
			};

			snapshots.push(SnapshotInfo {
				id: snapshot_id,
				timestamp
			});
		}

		Ok(snapshots)
	}

	pub(crate) async fn load_revisions(
		&self,
		doc_id: &str,
		snapshot_id: u64
	) -> Result<Vec<Revision>, StorageError> {
		let rev_file_name = format!("{}{}", snapshot_id, REV_NAME_ID);
		let file_path = format!("{}/{}/{}", self.docs_folder, doc_id, rev_file_name);

		let mut file = match File::open(&file_path).await {
			Ok(f) => f,
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to load revisions file '{}': {}", file_path, e
				)))
			}
		};

		let mut revs = Vec::new();

		while let Ok(n) = file.read(&mut [0; 1]).await {
			if n == 0 {
				break;
			}

			match file.seek(SeekFrom::Current(-1)).await {
				Ok(_) => (),
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to seek to previous byte: {}",
						e
					)))
				}
			}

			let mut rev_size_bytes = [0; 4];

			let rev_size = match file.read_exact(&mut rev_size_bytes).await {
				Ok(_) => u32::from_le_bytes(rev_size_bytes) as u32,
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to read revision size: {}",
						e
					)))
				}
			};

			let mut timestamp_bytes = [0; 8];
			let timestamp = match file.read_exact(&mut timestamp_bytes).await {
				Ok(_) => u64::from_le_bytes(timestamp_bytes),
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to read revision timestamp: {}",
						e
					)))
				}
			};

			let rev_data_size = rev_size - 8;

			let mut rev_data = vec![0; rev_data_size as usize];

			match file.read_exact(&mut rev_data).await {
				Ok(_) => (),
				Err(e) => {
					return Err(StorageError::LoadError(format!(
						"Failed to read revision data: {}",
						e
					)))
				}
			};

			revs.push(Revision {
				timestamp,
				data: rev_data
			});
		}

		Ok(revs)
	}

	pub(crate) async fn save_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64,
		snapshot: &Snapshot
	) -> Result<(), StorageError> {
		let file_path = format!("{}/{}/{}{}", self.docs_folder, doc_id, snapshot_id, SNAP_NAME_ID);
		let mut file = match File::create(&file_path).await {
			Ok(f) => f,
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to create snapshot file: {}",
					e
				)))
			}
		};

		let timestamp_bytes: [u8; 8] = snapshot.timestamp.to_be_bytes();

		match file.write_all(&timestamp_bytes).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to write timestamp: {}",
					e
				)))
			}
		};

		match file.write_all(&snapshot.data).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to write snapshot data: {}",
					e
				)))
			}
		};

		Ok(())
	}

	pub(crate) async fn save_revision(
		&self,
		doc_id: &str,
		revision_id: u64,
		revision: &Revision
	) -> Result<(), StorageError> {
		let rev_file_id = format!("{}{}", revision_id, REV_NAME_ID);
		let file_path = format!("{}/{}/{}", self.docs_folder, doc_id, rev_file_id);

		let mut file = match OpenOptions::new()
			.write(true)
			.create(true)
			.append(true)
			.open(file_path)
			.await {
			Ok(f) => f,
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to open revision file: {}",
					e
				)))
			}
		};

		let timestamp_bytes = revision.timestamp.to_le_bytes();

		match file.write_all(
			&(revision.data.len() as u32 + timestamp_bytes.len() as u32).to_le_bytes()
		).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to write revision size: {}",
					e
				)))
			}
		};

		match file.write_all(&timestamp_bytes).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to write revision timestamp: {}",
					e
				)))
			}
		};

		match file.write_all(&revision.data).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to write revision data: {}",
					e
				)))
			}
		};

		Ok(())
	}

	pub(crate) async fn load_metadata(&self, doc_id: &str) -> Result<DocMetadata, StorageError> {
		let file_path = format!("{}/{}/doc.json", self.docs_folder, doc_id);

		log::info!("Loading doc metadata from: {}", file_path);

		let mut file = match File::open(file_path).await {
			Ok(f) => f,
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to load metadata file: {}",
					e
				)))
			}
		};

		let mut buffer = Vec::new();
		match file.read_to_end(&mut buffer).await {
			Ok(_) => (),
			Err(e) => {
				return Err(StorageError::LoadError(format!(
					"Failed to read metadata file: {}",
					e
				)))
			}
		};

		return match serde_json::from_slice::<DocMetadata>(&buffer) {
			Ok(m) => Ok(m),
			Err(e) => Err(StorageError::LoadError(format!(
				"Failed to parse metadata file: {}",
				e
			)))
		}
	}

	pub(crate) async fn save_metadata(&self, doc_id: &str, metadata: &DocMetadata) -> Result<(), StorageError> {
		let file_path = format!("{}/{}/doc.json", self.docs_folder, doc_id);

		log::info!("Saving doc metadata at: {}. {:?}", file_path, metadata);

		let mut file = match File::create(file_path).await {
			Ok(f) => f,
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to create metadata file: {}",
					e
				)))
			}
		};

		let data = match &serde_json::to_vec(metadata) {
			Ok(d) => d.clone(),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to serialize metadata: {}",
					e
				)))
			}
		};

		match file.write_all(&data).await {
			Ok(_) => Ok(()),
			Err(e) => {
				return Err(StorageError::SaveError(format!(
					"Failed to write metadata file: {}",
					e
				)))
			}
		}
	}
}
