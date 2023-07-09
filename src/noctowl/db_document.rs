use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use log::LevelFilter;
use serde::{Deserialize, Serialize};
use sqlx::{Connection, ConnectOptions, Pool, Sqlite, SqlitePool};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::{nlog};
use crate::common::utils;
use crate::noctowl::db_project::does_document_exist;
use crate::noctowl::db_utils::{create_short_lived_connection_pool, db_exists, get_db_path};
use crate::noctowl::document::{Document, YrsUpdateStatus};
use crate::noctowl::NoctowlError;

const CREATE_CHECKPOINT_TABLE_QUERY: &str = r#"
CREATE TABLE checkpoints (
	checkpoint_id INTEGER PRIMARY KEY,
	snapshot_id INTEGER NOT NULL,
	update_index INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	short_description TEXT NOT NULL,
	long_description TEXT,
	FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id)
)
"#;

const CREATE_SNAPSHOTS_TABLE_QUERY: &str = r#"
CREATE TABLE snapshots (
	snapshot_id INTEGER PRIMARY KEY,
	created_at INTEGER NOT NULL,
	snapshot_data BLOB NOT NULL
)
"#;

const CREATE_UPDATES_TABLE_QUERY: &str = r#"
CREATE TABLE updates (
	update_id INTEGER PRIMARY KEY,
	snapshot_id INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	update_data BLOB NOT NULL,
	FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id)
)
"#;

const CREATE_DOC_INFO_TABLE_QUERY: &str = r#"
CREATE TABLE doc (
	doc_id INTEGER PRIMARY KEY,
	doc_pid TEXT UNIQUE NOT NULL,
	last_modified INTEGER NOT NULL,
	latest_snapshot_id INTEGER NOT NULL,
	updates_for_current_snapshot INTEGER NOT NULL,
	FOREIGN KEY(latest_snapshot_id) REFERENCES snapshots(snapshot_id)
)
"#;

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocInfo {
	pub doc_id: i32,
	pub doc_pid: String,
	pub last_modified: i64,
	pub latest_snapshot_id: i64,
	pub updates_for_current_snapshot: i32,
}

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSnapshot {
	pub snapshot_id: i64,
	pub created_at: i64,
	pub snapshot_data: Vec<u8>,
}

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocUpdate {
	pub update_id: i64,
	pub snapshot_id: i64,
	pub created_at: i64,
	pub update_data: Vec<u8>,
}

#[derive(sqlx::FromRow)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocCheckpoint {
	pub checkpoint_id: i64,
	pub snapshot_id: i64,
	pub update_index: i32,
	pub created_at: i64,
	pub short_description: String,
	pub long_description: Option<String>,
}

/**
 * Get a connection to a document database.
 *
 * This function will check if the document exists in the project database, and if it does, it will
 * check if we have a local copy of the document's database. If we do, we will return a connection
 * to that database. If we don't, we will fetch the database from AWS S3, load it, and return a
 * connection to it.
 *
 * If the document does not exist in the project database, this function will return an error.
 */
pub async fn get_doc_db_connection(
	project_db_pool: &Pool<Sqlite>,
	connection_pools: &Arc<RwLock<HashMap<String, SqlitePool>>>,
	access_map: &Arc<RwLock<HashMap<String, Instant>>>,
	users_dir: &str,
	doc_pid: &str,
	user_pid: &str,
) -> Result<Pool<Sqlite>, NoctowlError> {
	{
		access_map.write().await.insert(doc_pid.to_string(), Instant::now());
	}

	let mut cache = connection_pools.write().await;

	match cache.entry(doc_pid.to_string()) {
		Entry::Occupied(entry) => Ok(entry.get().clone()),
		Entry::Vacant(entry) => {
			let connection_pool = new_doc_db_connection(
				project_db_pool,
				users_dir,
				doc_pid,
				user_pid,
			).await?;

			entry.insert(connection_pool.clone());

			Ok(connection_pool)
		}
	}
}

async fn new_doc_db_connection(
	project_db_pool: &Pool<Sqlite>,
	users_dir: &str,
	doc_pid: &str,
	user_pid: &str,
) -> Result<Pool<Sqlite>, NoctowlError> {
	// first we check if the doc exists on the project database
	if !does_document_exist(project_db_pool, doc_pid).await.unwrap_or(false) {
		return Err(NoctowlError::DocumentNotFound(doc_pid.to_string()));
	}

	// now let's check if we have a local copy of the project's database
	let db_path = get_db_path(users_dir, user_pid, doc_pid);

	if !db_exists(&db_path).await {
		// we need to fetch the database from AWS S3 before proceeding
	}

	// load the database and get a connection pool
	let connection_pool = create_short_lived_connection_pool(
		&db_path,
		None,
		None,
	).await?;

	Ok(connection_pool)
}

pub async fn create_doc_db_and_insert(
	file_path: &str,
	doc_pid: &str,
	current_timestamp: i64,
	snapshot_data: &[u8],
) -> Result<(), NoctowlError> {
	nlog!("Creating doc database {}", file_path);

	let mut conn = SqliteConnectOptions::new()
		.filename(file_path)
		.journal_mode(SqliteJournalMode::Wal)
		.read_only(false)
		.create_if_missing(true)
		.log_statements(LevelFilter::Debug)
		.connect()
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating doc database", e))?;

	let mut tx = conn.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	sqlx::query(CREATE_SNAPSHOTS_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating snapshots table", e))?;

	sqlx::query(CREATE_UPDATES_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating updates table", e))?;

	sqlx::query(CREATE_CHECKPOINT_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating checkpoints table", e))?;

	sqlx::query(CREATE_DOC_INFO_TABLE_QUERY)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error creating doc table", e))?;

	// add first snapshot
	let res = sqlx::query("INSERT INTO snapshots (created_at, snapshot_data) VALUES (?, ?)")
		.bind(current_timestamp)
		.bind(snapshot_data)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting into snapshots table", e))?;

	let last_snap_id = res.last_insert_rowid();

	// Insert into doc table
	sqlx::query(
		r#"
            INSERT INTO doc (doc_pid, last_modified, latest_snapshot_id, updates_for_current_snapshot)
            VALUES (?, ?, ?, ?)
        "#,
	)
		.bind(doc_pid)
		.bind(current_timestamp)
		.bind(last_snap_id)
		.bind(0)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting into doc table", e))?;

	// Commit the transaction
	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	conn.close().await
		.map_err(|e| NoctowlError::SqlxErrorClosingConnection(e))?;

	nlog!("Tables created successfully at {} and initial data inserted", file_path);

	Ok(())
}

pub async fn get_doc(
	doc_db_pool: &Pool<Sqlite>,
	doc_pid: &str,
	snapshot_id: &Option<i64>,
	update_index: &Option<i64>,
) -> Result<(DocSnapshot, Option<Vec<DocUpdate>>), NoctowlError> {
	// Begin a transaction
	let mut tx = doc_db_pool.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	// Get the snapshot snapshot_id or the latest snapshot if snapshot_id is none
	let snapshot: Option<DocSnapshot> = if snapshot_id.is_none() {
		sqlx::query_as(
			r#"
        SELECT * FROM doc
        INNER JOIN snapshots ON doc.latest_snapshot_id = snapshots.snapshot_id
        WHERE doc_pid = ?
        "#,
		)
			.bind(doc_pid)
			.fetch_optional(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error getting latest snapshot", e))?
	} else {
		sqlx::query_as("SELECT * FROM snapshots WHERE snapshot_id = ?")
			.bind(snapshot_id.unwrap())
			.fetch_optional(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error getting specific snapshot", e))?
	};

	if snapshot.is_none() {
		tx.rollback().await
			.map_err(|e| NoctowlError::SqlxError("Error rolling back transaction", e))?;

		return Err(NoctowlError::DocumentNotFound("Document with specified snapshot_id not found".to_string()));
	}

	let snapshot = snapshot.unwrap();

	// Get associated updates, if any
	let updates: Vec<DocUpdate> = if update_index.is_none() {
		sqlx::query_as(
			r#"
        SELECT * FROM updates
        WHERE snapshot_id = ?
        "#,
		)
			.bind(snapshot.snapshot_id)
			.fetch_all(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error getting updates", e))?
	} else {
		sqlx::query_as(
			r#"
        SELECT * FROM updates
        WHERE snapshot_id = ? AND update_id <= ?
        "#,
		)
			.bind(snapshot.snapshot_id)
			.bind(update_index.unwrap())
			.fetch_all(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error getting updates", e))?
	};

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok((snapshot, if updates.is_empty() { None } else { Some(updates) }))
}

/**
 * Get a document from the cache, or get it from the database and add it to the cache.
 */
pub async fn get_document(
	doc_db_pool: &Pool<Sqlite>,
	docs_cache: &Arc<RwLock<HashMap<String, Arc<Mutex<Document>>>>>,
	doc_pid: &str,
	snapshot_id: &Option<i64>,
	update_index: &Option<i64>,
) -> Result<Arc<Mutex<Document>>, NoctowlError> {
	let entry_key = format!("{}_{}_{}", doc_pid, snapshot_id.unwrap_or(-1), update_index.unwrap_or(-1));

	let mut cache = docs_cache.write().await;

	match cache.entry(entry_key) {
		Entry::Occupied(entry) => Ok(entry.get().clone()),
		Entry::Vacant(entry) => {
			let (snapshot, updates) = get_doc(doc_db_pool, doc_pid, snapshot_id, update_index).await?;

			let doc = Document::new_from_snapshot(
				doc_pid,
				snapshot,
				updates,
			);

			let doc = Arc::new(Mutex::new(doc));

			entry.insert(doc.clone());

			Ok(doc)
		}
	}
}

pub async fn get_previous_document(
	doc_db_pool: &Pool<Sqlite>,
	docs_cache: &Arc<RwLock<HashMap<String, Arc<Mutex<Document>>>>>,
	doc_pid: &str,
	prev_amount: i32,
) -> Result<Arc<Mutex<Document>>, NoctowlError> {
	let mut tx = doc_db_pool.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	let doc_info: DocInfo = sqlx::query_as("SELECT * FROM doc WHERE doc_pid = ?")
		.bind(doc_pid)
		.fetch_one(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error getting doc info", e))?;

	let updates_for_current_snapshot = doc_info.updates_for_current_snapshot as i32;

	let (prev_snap_id, prev_update_index) = if updates_for_current_snapshot > prev_amount {
		(Some(doc_info.latest_snapshot_id), Some((updates_for_current_snapshot - prev_amount) as i64))
	} else {
		let prev_snap_id = if doc_info.latest_snapshot_id > 1 {
			Some(doc_info.latest_snapshot_id - 1)
		} else {
			None
		};

		let prev_update_index: Option<i64> = sqlx::query_scalar(
			"SELECT MAX(update_index) FROM updates WHERE snapshot_id = ?"
		).bind(prev_snap_id.unwrap_or(-1))
			.fetch_optional(&mut tx).await
			.map_err(|e| NoctowlError::SqlxError("Error getting latest update index for the previous snapshot", e))?;

		let prev_update_index = if prev_update_index.is_some() {
			let prev_amount = prev_amount - updates_for_current_snapshot;
			let prev_update_index = prev_update_index.unwrap();

			if prev_update_index > prev_amount as i64 {
				Some(prev_update_index - prev_amount as i64)
			} else {
				Some(prev_update_index)
			}
		} else {
			None
		};

		(prev_snap_id, prev_update_index)
	};

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	get_document(doc_db_pool, docs_cache, doc_pid, &prev_snap_id, &prev_update_index).await
}

pub async fn save_doc_update(
	doc_db_pool: &Pool<Sqlite>,
	doc_pid: &str,
	update: &Vec<u8>,
	full_doc_state: &Vec<u8>,
) -> Result<(), NoctowlError> {
	let mut tx = doc_db_pool.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	let current_timestamp = utils::current_timestamp_secs() as i64;

	let (latest_snapshot_id, updates_for_current_snapshot): (i64, i32) = sqlx::query_as(
		r#"
        SELECT latest_snapshot_id, updates_for_current_snapshot FROM doc
        WHERE doc_pid = ?
    "#,
	)
		.bind(&doc_pid)
		.fetch_one(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error getting latest snapshot id", e))?;

	// First let's insert the update to the current snapshot
	{
		sqlx::query(
			r#"
            INSERT INTO updates (snapshot_id, created_at, update_data)
            VALUES (?, ?, ?)
        "#,
		)
			.bind(latest_snapshot_id)
			.bind(current_timestamp)
			.bind(update)
			.execute(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error inserting into updates table", e))?;
	}

	// this should come from a env variable
	let max_updates_for_each_snapshot = 150;

	// If we haven't reached the max number of updates for the current snapshot, we only update the doc table
	if updates_for_current_snapshot < max_updates_for_each_snapshot {
		sqlx::query(
			r#"
        UPDATE doc
        SET updates_for_current_snapshot = ?, last_modified = ?
        WHERE doc_pid = ?
    "#,
		)
			.bind(updates_for_current_snapshot + 1)
			.bind(current_timestamp)
			.bind(&doc_pid)
			.execute(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error updating doc table", e))?;
	} else {
		// Otherwise we also insert a new snapshot and reset the updates_for_current_snapshot counter

		let snapshot_data = full_doc_state;

		let res = sqlx::query(
			r#"
            INSERT INTO snapshots (created_at, snapshot_data)
            VALUES (?, ?)
        "#,
		)
			.bind(current_timestamp)
			.bind(snapshot_data)
			.execute(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error inserting into snapshots table", e))?;

		let new_latest_snapshot_id = res.last_insert_rowid();
		let new_updates_for_current_snapshot = 0;

		sqlx::query(
			r#"
        UPDATE doc
        SET latest_snapshot_id = ?, updates_for_current_snapshot = ?, last_modified = ?
        WHERE doc_pid = ?
    "#,
		)
			.bind(new_latest_snapshot_id)
			.bind(new_updates_for_current_snapshot)
			.bind(current_timestamp)
			.bind(&doc_pid)
			.execute(&mut tx)
			.await
			.map_err(|e| NoctowlError::SqlxError("Error updating doc table", e))?;
	}

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok(())
}

pub async fn process_update(
	update: &Vec<u8>,
	docs_cache: &Arc<RwLock<HashMap<String, Arc<Mutex<Document>>>>>,
	access_map: &Arc<RwLock<HashMap<String, Instant>>>,
	users_dir: &str,
	doc_pid: &str,
	user_pid: &str,
	project_db_pool: &Pool<Sqlite>,
	docs_db_pool: &Arc<RwLock<HashMap<String, SqlitePool>>>,
) -> Result<YrsUpdateStatus, NoctowlError> {
	let doc_db_pool = get_doc_db_connection(
		&project_db_pool,
		&docs_db_pool,
		access_map,
		users_dir,
		doc_pid,
		user_pid,
	).await?;

	let doc = get_document(
		&doc_db_pool,
		&docs_cache,
		doc_pid,
		&None,
		&None,
	).await?;

	let max_retries = 50;
	let mut delay = 5;

	for _ in 0..max_retries {
		{
			let mut document = doc.lock().await;

			let (new_state, status) = match catch_unwind(AssertUnwindSafe(|| {
				document.try_atomic_apply_update_and_get(&update)
			})) {
				Ok(res) => res,
				Err(e) => {
					// cuando pasa esto, tendríamos que decirle al cliente
					// que nos pase el estado completo, y generamos un snapshot nuevo
					// probamos hasta cuál update se puede aplicar al snapshot anterior
					// y borramos las updates corruptas.
					nlog!("Error applying update: {:?}", e);

					return Err(NoctowlError::DocumentUpdateFailed("Error applying update".to_string()));
				}
			};

			if status == YrsUpdateStatus::Failed {
				nlog!("Update failed.");

				return Err(NoctowlError::DocumentUpdateFailed("YrsUpdateStatus::Failed".to_string()));
			}

			if status == YrsUpdateStatus::Busy {
				nlog!("Update pending. Retrying in {}ms.", delay);
			} else {
				if let Some(new_state) = new_state {
					save_doc_update(
						&doc_db_pool,
						doc_pid,
						&update,
						&new_state,
					).await.unwrap();
				}

				return Ok(status);
			}
		}

		// Sleep for delay milliseconds
		tokio::time::sleep(std::time::Duration::from_millis(delay as u64)).await;

		// Exponential backoff: double the delay for the next iteration
		delay *= 2;
	};

	// If we reach this point, all retries have failed.
	return Err(NoctowlError::DocumentUpdateFailed(doc_pid.to_string()));
}

pub async fn get_snapshots_between_timestamps(
	doc_db_pool: &Pool<Sqlite>,
	timestamp_since: i64,
	timestamp_until: i64,
) -> Result<Vec<(i64, i64)>, NoctowlError> {
	// Begin a transaction
	let mut tx = doc_db_pool.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	// Get the snapshots
	let snapshots: Vec<(i64, i64)> = sqlx::query_as(
		r#"
        SELECT snapshot_id, created_at FROM snapshots
        WHERE created_at >= ? AND created_at <= ?
        "#,
	)
		.bind(timestamp_since)
		.bind(timestamp_until)
		.fetch_all(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error getting snapshots", e))?;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok(snapshots)
}

pub async fn get_updates(
	doc_db_pool: &Pool<Sqlite>,
	snapshot_id: i64,
	from_id: i64,
	amount: usize,
) -> Result<Vec<(i64, i64)>, NoctowlError> {
	// Begin a transaction
	let mut tx = doc_db_pool.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;

	// Get total updates
	let total_updates: i64 = sqlx::query_scalar(
		r#"
        SELECT COUNT(*) FROM updates
        WHERE snapshot_id = ?
        "#,
	)
		.bind(snapshot_id)
		.fetch_one(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error getting total updates", e))?;

	let end_id = if total_updates - from_id < amount as i64 {
		total_updates
	} else {
		from_id + amount as i64
	};

	// Get the updates
	let updates: Vec<(i64, i64)> = sqlx::query_as(
		r#"
        SELECT update_id, created_at FROM updates
        WHERE snapshot_id = ? AND update_id >= ? AND update_id <= ?
        ORDER BY update_id
        "#,
	)
		.bind(snapshot_id)
		.bind(from_id)
		.bind(end_id)
		.fetch_all(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error getting updates", e))?;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok(updates)
}

pub async fn save_checkpoint(
	doc_db_pool: &Pool<Sqlite>,
	doc_pid: &str,
	short_description: String,
	long_description: Option<String>,
	snapshot_id: Option<i64>,
	update_index: Option<i32>,
) -> Result<(), NoctowlError> {
	let mut tx = doc_db_pool.begin().await
		.map_err(|e| NoctowlError::SqlxError("Error beginning transaction", e))?;
	let current_timestamp = utils::current_timestamp_secs() as i64;

	let (default_snapshot_id, default_update_index): (i64, i32) = sqlx::query_as(
		r#"
        SELECT latest_snapshot_id, updates_for_current_snapshot FROM doc
        WHERE doc_pid = ?
        "#,
	)
		.bind(&doc_pid)
		.fetch_one(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error getting default values from doc table", e))?;

	let final_snapshot_id = snapshot_id.unwrap_or(default_snapshot_id);
	let final_update_index = update_index.unwrap_or(default_update_index);

	sqlx::query(
		r#"
        INSERT INTO checkpoints (snapshot_id, update_index, created_at, short_description, long_description)
        VALUES (?, ?, ?, ?, ?)
        "#,
	)
		.bind(final_snapshot_id)
		.bind(final_update_index)
		.bind(current_timestamp)
		.bind(short_description)
		.bind(long_description)
		.execute(&mut tx)
		.await
		.map_err(|e| NoctowlError::SqlxError("Error inserting into checkpoints table", e))?;

	tx.commit().await
		.map_err(|e| NoctowlError::SqlxError("Error committing transaction", e))?;

	Ok(())
}

pub async fn get_all_updates(
	doc_db_pool: &Pool<Sqlite>
) -> Result<Vec<DocUpdate>, NoctowlError> {
	let rows: Vec<DocUpdate> = sqlx::query_as("SELECT * FROM updates")
		.fetch_all(doc_db_pool)
		.await
		.map_err(|e| NoctowlError::SqlxError("Failed to get all updates", e))?;

	Ok(rows)
}
