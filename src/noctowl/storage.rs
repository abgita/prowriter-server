use std::fmt;

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
