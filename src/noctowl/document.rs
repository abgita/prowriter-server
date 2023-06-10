use std::fmt::{Debug, Display, Formatter};
use yrs::{merge_updates_v1, ReadTxn, StateVector, Transact, Update};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use crate::nlog;
use crate::noctowl::db_document::{DocSnapshot, DocUpdate};

#[derive(Debug, Clone)]
pub struct Document {
	pub id: String,
	pub ydoc: yrs::Doc
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
		let mut doc = Document::new(name);

		doc.apply_update(&snapshot.snapshot_data);

		if let Some(updates) = updates {
			doc.apply_updates(updates);
		}

		doc
	}

	pub fn get_doc_state(&self) -> Vec<u8> {
		self.ydoc
			.try_transact()
			.expect("another read-write transaction is in progress")
			.encode_state_as_update_v1(&StateVector::default())
	}

	pub fn apply_update(&mut self, message: &Vec<u8>) {
		self.ydoc
			.try_transact_mut().expect("another transaction is in progress")
			.apply_update(Update::decode_v1(message).unwrap());
	}

	pub fn try_atomic_apply_update_and_get(&mut self, message: &Vec<u8>) -> (Vec<u8>, Vec<u8>) {
		let mut tx = self.ydoc
			.try_transact_mut()
			.expect("another transaction is in progress");

		let prev = tx.encode_state_as_update_v1(&StateVector::default());

		let update = Update::decode_v1(message).unwrap();
		tx.apply_update(update);

		let curr = tx.encode_state_as_update_v1(&StateVector::default());

		(prev, curr)
	}

	pub fn apply_updates(&mut self, messages: Vec<DocUpdate>) {
		// we should measure which is or more correct and faster later
		let should_merge_updates = true;

		if should_merge_updates {
			let messages: Vec<&[u8]> = messages.iter().map(|update| update.update_data.as_ref()).collect();
			let merged_updates = merge_updates_v1(&messages).unwrap();

			self.ydoc
				.transact_mut()
				.apply_update(Update::decode_v1(&merged_updates).unwrap());
		} else {
			for message in messages {
				self.ydoc
					.transact_mut()
					.apply_update(Update::decode_v1(&message.update_data).unwrap());
			}
		}
	}

	pub fn log(&self) {
		nlog!("{}", &self);
	}
}

impl Display for Document {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "({}: {})", &self.id, &self.ydoc.to_json(&self.ydoc.transact()))
	}
}
