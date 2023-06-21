use std::fmt::{Debug, Display, Formatter};
use yrs::{merge_updates_v1, ReadTxn, StateVector, Transact, Update};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use crate::{nlog};
use crate::noctowl::db_document::{DocSnapshot, DocUpdate};

#[derive(Debug, Clone)]
pub struct Document {
	pub id: String,
	pub ydoc: yrs::Doc
}

#[derive(PartialEq, Debug, Clone)]
pub enum YrsUpdateStatus {
	Updated,
	Pending(Vec<u8>),
	Busy,
	NoUpdate,
	Failed,
}

impl Document {
	pub fn new(name: &str) -> Self {
		let yrs_doc = yrs::Doc::new();

		yrs_doc.get_or_insert_xml_text("root");

		Self {
			id: String::from(name),
			ydoc: yrs_doc,
		}
	}

	pub fn new_from_snapshot(
		name: &str,
		snapshot: DocSnapshot,
		updates: Option<Vec<DocUpdate>>
	) -> Self {
		let doc = Document::new(name);

		{
			let mut tx = doc.ydoc
				.try_transact_mut().expect("another transaction is in progress");

			tx.apply_update(Update::decode_v1(&snapshot.snapshot_data).unwrap());

			if let Some(updates) = updates {
				let messages: Vec<&[u8]> = updates.iter().map(|update| update.update_data.as_ref()).collect();
				let merged_updates = merge_updates_v1(&messages).unwrap();

				tx.apply_update(Update::decode_v1(&merged_updates).unwrap());
			}
		}

		doc
	}

	pub fn get_doc_state(&self) -> Vec<u8> {
		self.ydoc
			.try_transact()
			.expect("another read-write transaction is in progress")
			.encode_state_as_update_v1(&StateVector::default())
	}

	pub fn try_atomic_apply_update_and_get(
		&mut self,
		message: &Vec<u8>
	) -> (Option<Vec<u8>>, YrsUpdateStatus) {
		let mut tx = match self.ydoc.try_transact_mut() {
			Ok(tx) => tx,
			Err(e) => {
				nlog!("Error getting transaction: {}", e);

				return (None, YrsUpdateStatus::Busy);
			},
		};

		let update = match Update::decode_v1(message) {
			Ok(update) => update,
			Err(e) => {
				nlog!("Error decoding update: {}", e);

				return (None, YrsUpdateStatus::Failed);
			},
		};

		let are_sv_equal = update.state_vector() == tx.state_vector();

		let update_before = tx.encode_update_v1();
		tx.apply_update(update);
		let update_after = tx.encode_update_v1();

		let are_updates_equal = update_before == update_after;

		// https://github.com/y-crdt/y-crdt/issues/297
		let status = if !are_sv_equal && !are_updates_equal {
			YrsUpdateStatus::Updated
		} else if are_sv_equal && are_updates_equal {
			YrsUpdateStatus::NoUpdate
		} else if !are_sv_equal && are_updates_equal {
			// if there are missing updates, we send the client our state vector
			let sv = tx.state_vector().encode_v1();

			YrsUpdateStatus::Pending(sv)
		} else {
			// I don't know how to treat this state
			YrsUpdateStatus::Failed
		};

		let state = if status == YrsUpdateStatus::Updated {
			Some(tx.encode_state_as_update_v1(&StateVector::default()))
		} else {
			None
		};

		(state, status)
	}
}

impl Display for Document {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "({}: {})", &self.id, &self.ydoc.to_json(&self.ydoc.transact()))
	}
}
