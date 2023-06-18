mod testing;
pub mod db_main;
pub mod db_project;
pub mod db_document;
mod db_utils;
pub mod document;
pub mod lib;

use std::fmt::{Debug, Display, Formatter};

#[macro_export]
macro_rules! nlog {
	($($arg:tt)*) => (log::info!(target: "noctowl", $($arg)*))
}

#[derive(PartialEq, Debug)]
pub enum NoctowlStatus {
	Ok,
	UserAlreadyExists,
	UserNotFound,
	ProjectAlreadyExists,
	ProjectNotFound,
	DocumentAlreadyExists,
	DocumentNotFound,
}

pub enum NoctowlError {
	Error(&'static str, Box<dyn std::error::Error + Send + Sync>),
	IoError(String),
	ProjectNotFound(String),
	DocumentNotFound(String),
	DocumentUpdateFailed(String),
	SqlxError(&'static str, sqlx::Error),
	SqlxErrorClosingConnection(sqlx::Error),
}

impl Display for NoctowlError {
	fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
		match self {
			NoctowlError::Error(s, e) => write!(f, "{}: {}", s, e),
			NoctowlError::IoError(s) => write!(f, "{}", s),
			NoctowlError::ProjectNotFound(s) => write!(f, "Project not found: {}", s),
			NoctowlError::DocumentNotFound(s) => write!(f, "Document not found: {}", s),
			NoctowlError::DocumentUpdateFailed(s) => write!(f, "Error updating document: {}", s),
			NoctowlError::SqlxError(s, e) => write!(f, "{}: {}", s, e),
			NoctowlError::SqlxErrorClosingConnection(e) => write!(f, "Error closing connection: {}", e),
		}
	}
}

impl Debug for NoctowlError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}

impl std::error::Error for NoctowlError {}
