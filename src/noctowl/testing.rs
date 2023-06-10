use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use yrs::{Text, Transact, TransactionMut};
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::sync::RwLock;
use crate::common::utils::get_new_user_pid;
use crate::noctowl::document::Document;
use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlError;

pub fn get_test_updates(shuffled: bool, number: usize) -> Vec<Vec<u8>> {
	let doc = Document::new("test");
	let ydoc = &doc.ydoc;

	let updates = Rc::new(RefCell::new(Vec::new()));

	let updates_ref = updates.clone();

	let _sub = ydoc.observe_update_v1(move |_: &TransactionMut, e| {
		updates_ref.borrow_mut().push(e.update.clone());
	}).unwrap();

	let root = ydoc.get_or_insert_xml_text("root");

	for i in 0..number {
		let txn = &mut ydoc.transact_mut();

		root.push(txn, format!("\nline_{}", i).as_str());
	}

	if shuffled {
		let mut rng = thread_rng();
		let mut list = updates.take();
		list.shuffle(&mut rng);
		list
	} else {
		updates.take()
	}
}

pub async fn get_test_users(
	noctowl: &Arc<RwLock<Noctowl>>,
	amount: usize
) -> Result<Vec<(String, String, String)>, NoctowlError> {
	let noctowl = noctowl.write().await;
	let mut users = Vec::new();

	for _i in 0..amount {
		let user_pid = get_new_user_pid();
		let (project_pid, _) = noctowl.create_project(&user_pid, "New Project").await?;
		let (doc_row, _) = noctowl.create_document(&user_pid, &project_pid, "Test Doc", None, None).await?;
		let doc_pid = doc_row.unwrap().doc_pid;

		users.push((user_pid, project_pid, doc_pid));
	}

	Ok(users)
}
