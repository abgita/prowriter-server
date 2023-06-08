use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub enum StorageError {
	LoadError(String),
	SaveError(String),
	ConnectionError(String),
	CreationError(String),
}

impl fmt::Display for StorageError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			StorageError::LoadError(s) => write!(f, "LoadError: {}", s),
			StorageError::SaveError(s) => write!(f, "SaveError: {}", s),
			StorageError::ConnectionError(s) => write!(f, "ConnectionError: {}", s),
			StorageError::CreationError(s) => write!(f, "CreationError: {}", s),
		}
	}
}

impl fmt::Debug for StorageError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Display::fmt(self, f)
	}
}

impl std::error::Error for StorageError {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DocMetadata {
	#[serde(rename = "docId")]
	pub doc_id: String,
	#[serde(rename = "currentRevision")]
	pub current_revision: u64,
	#[serde(rename = "latestSnapshot")]
	pub latest_snapshot: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Revision {
	pub timestamp: u64,
	pub data: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Snapshot {
	pub timestamp: u64,
	pub data: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotInfo {
	pub id: u64,
	pub timestamp: u64,
}

#[async_trait]
pub trait StorageBackend: Send + Sync {
	async fn create_doc(
		&self,
		doc_id: &str,
		snapshot: &Snapshot,
	) -> Result<String, StorageError>;

	async fn load_doc(
		&self,
		doc_id: &str,
		snapshot_id: i64,
	) -> Result<(Snapshot, Option<Vec<Revision>>), StorageError>;

	async fn load_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64,
	) -> Result<Snapshot, StorageError>;

	async fn get_latest_snapshots(
		&self,
		doc_id: &str,
		amount: usize,
	) -> Result<Vec<SnapshotInfo>, StorageError>;

	async fn load_revisions(
		&self,
		doc_id: &str,
		snapshot_id: u64,
	) -> Result<Vec<Revision>, StorageError>;

	async fn save_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64,
		snapshot: &Snapshot,
	) -> Result<(), StorageError>;

	async fn save_revision(
		&self,
		doc_id: &str,
		revision_id: u64,
		revision: &Revision,
	) -> Result<(), StorageError>;

	async fn load_metadata(
		&self,
		doc_id: &str,
	) -> Result<DocMetadata, StorageError>;

	async fn save_metadata(
		&self,
		doc_id: &str,
		metadata: &DocMetadata,
	) -> Result<(), StorageError>;
}
