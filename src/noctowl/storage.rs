use async_trait::async_trait;
use std::collections::HashMap;
use std::{fmt};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io;
use tokio::sync::RwLock;
use yrs::{Doc};

pub enum StorageError {
	LoadError(String),
	SaveError(String),
	DeleteError(String),
}

impl fmt::Display for StorageError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			StorageError::LoadError(s) => write!(f, "LoadError: {}", s),
			StorageError::SaveError(s) => write!(f, "SaveError: {}", s),
			StorageError::DeleteError(s) => write!(f, "DeleteError: {}", s),
		}
	}
}

impl fmt::Debug for StorageError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Display::fmt(self, f)
	}
}

impl std::error::Error for StorageError {}

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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DocMetadata {
	#[serde(rename = "docId")]
	pub doc_id: String,
	#[serde(rename = "currentRevision")]
	pub current_revision: u64,
	#[serde(rename = "latestSnapshot")]
	pub latest_snapshot: u64,
}

pub struct DocManager {
	pub cache: HashMap<String, Arc<RwLock<Document>>>,
	pub storage_backend: Arc<dyn StorageBackend + Send + Sync>,
}

pub struct Revision {
	pub timestamp: u64,
	pub data: Vec<u8>,
}

pub struct Snapshot {
	pub timestamp: u64,
	pub data: Vec<u8>,
}

pub struct SnapshotInfo {
	pub id: u64,
	pub timestamp: u64,
}

#[async_trait]
pub trait StorageBackend {
	async fn create_doc(
		&mut self,
		doc_id: &str,
		snapshot: &Snapshot,
	) -> Result<String, StorageError>;

	async fn load_doc(
		&self,
		doc_id: &str,
		snapshot_id: i64
	) -> Result<(Snapshot, Option<Vec<Revision>>), StorageError>;

	async fn load_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64
	) -> io::Result<Snapshot>;

	/*async fn get_revision(
		&self,
		doc_id: &str,
		revision_id: u64
	) -> Result<Vec<Revision>, Box<Error>>;*/

	async fn get_latest_snapshots(
		&self,
		doc_id: &str,
		amount: usize
	) -> Result<Vec<SnapshotInfo>, StorageError>;

	async fn load_revisions(
		&self,
		doc_id: &str,
		snapshot_id: u64
	) -> Result<Vec<Revision>, StorageError>;

	async fn save_snapshot(
		&self,
		doc_id: &str,
		snapshot_id: u64,
		snapshot: &Snapshot
	) -> Result<(), StorageError>;

	async fn save_revision(
		&self,
		doc_id: &str,
		revision_id: u64,
		revision: &Revision
	) -> Result<(), StorageError>;

	async fn load_metadata(
		&self,
		doc_id: &str
	) -> Result<DocMetadata, StorageError>;

	async fn save_metadata(
		&self,
		doc_id: &str,
		metadata: &DocMetadata
	) -> Result<(), StorageError>;
}
