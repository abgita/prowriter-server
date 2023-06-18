use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::{fs, task};
use tokio::sync::{Mutex, RwLock};
use yrs::{merge_updates_v1, Options, ReadTxn, StateVector, Text, Transact, TransactionMut, Update, Uuid, XmlTextRef};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;

use crate::common::utils::get_new_user_pid;
use crate::noctowl::db_document::DocUpdate;
use crate::noctowl::document::{Document, YrsUpdateStatus};
use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlError;

fn get_test_document() -> (Document, XmlTextRef) {
	let doc = Document::new("test");
	let ydoc = &doc.ydoc;

	let root = ydoc.get_or_insert_xml_text("root");

	(doc, root)
}

fn get_test_updates(shuffled: bool, number: usize) -> (Document, (Vec<Vec<u8>>, Vec<String>, Vec<Vec<u8>>)) {
	let doc = Document::new("test");
	let ydoc = &doc.ydoc;
	let ydoc = ydoc.clone();

	let updates = Rc::new(RefCell::new(Vec::new()));
	let snapshots = Rc::new(RefCell::new(Vec::new()));
	let sv = Rc::new(RefCell::new(Vec::new()));

	let updates_ref = updates.clone();
	let snapshots_ref = snapshots.clone();
	let sv_ref = sv.clone();
	let ydoc_ref = ydoc.clone();

	let _sub = ydoc.observe_update_v1(move |tx: &TransactionMut, e| {
		updates_ref.borrow_mut().push(e.update.clone());
		snapshots_ref.borrow_mut().push(ydoc_ref.to_json(tx).to_string());

		sv_ref.borrow_mut().push(tx.encode_diff_v1(&tx.state_vector()));
	}).unwrap();

	let root = ydoc.get_or_insert_xml_text("root");

	for i in 0..number {
		let txn = &mut ydoc.transact_mut();

		root.push(txn, format!("|line_{}", i).as_str());
	}

	let res = if shuffled {
		let mut rng = thread_rng();
		let mut list = updates.take();
		list.shuffle(&mut rng);
		list
	} else {
		updates.take()
	};

	(doc, (res, snapshots.take(), sv.take()))
}

fn get_test_updates_same_origin(number: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
	let ydoc = yrs::Doc::with_options(Options::with_guid_and_client_id(
		Uuid::from("test"),
		0,
	));

	let root = ydoc.get_or_insert_xml_text("root");

	let mut updates = Vec::new();
	let mut full_states = Vec::new();

	for i in 0..number {
		let txn = &mut ydoc.transact_mut();

		let sv = txn.state_vector();

		root.push(txn, format!("\nline_{}", i).as_str());

		updates.push(txn.encode_diff_v1(&sv));
		full_states.push(txn.encode_state_as_update_v1(&StateVector::default()));
	}

	(updates, full_states)
}

fn get_test_updates_safe(number: usize) -> (Vec<Vec<u8>>, Vec<String>) {
	let (updates, _) = get_test_updates_same_origin(number);

	let ydoc = yrs::Doc::with_options(Options::with_guid_and_client_id(
		Uuid::from("test"),
		0,
	));

	ydoc.get_or_insert_xml_text("root");

	let mut snapshots = Vec::new();

	for update in updates.iter() {
		let tx = &mut ydoc.transact_mut();

		let update_slice = update.as_slice();
		let update = Update::decode_v1(update_slice).unwrap();
		tx.apply_update(update);

		snapshots.push(ydoc.to_json(tx).to_string());
	}

	(updates, snapshots)
}

async fn get_doc_state(
	noctowl: &Noctowl,
	user_pid: &str,
	project_pid: &str,
	doc_pid: &str,
) -> Vec<u8> {
	let res = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await;
	let (doc, _) = res.unwrap();
	doc.unwrap().lock().await.get_doc_state()
}

async fn get_doc_json(
	noctowl: &Noctowl,
	user_pid: &str,
	project_pid: &str,
	doc_pid: &str,
) -> String {
	let res = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await;
	let (doc, _) = res.unwrap();
	doc.unwrap().lock().await.to_string()
}

async fn get_base_doc() -> Result<(Arc<Mutex<Document>>, Vec<DocUpdate>), NoctowlError> {
	let noctowl = Noctowl::new(Some("test_artifacts".to_string())).await?;
	let noctowl = Arc::new(RwLock::new(noctowl));
	let noctowl = noctowl.read().await;

	let user_pid = "test";
	let project_pid = "p439633";
	let doc_pid = "a374f61d-5419-4401-b22f-72b58bffbd9a";

	let doc = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?.0;
	let doc = doc.unwrap();

	let updates = noctowl.get_all_document_updates(&user_pid, &project_pid, &doc_pid).await?;

	noctowl.clean_up().await;

	Ok((doc, updates))
}

//--------------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_linear_regular() -> Result<(), NoctowlError> {
	let tmp_dir = ".test_storage/test_linear_regular".to_string();
	let noctowl = Noctowl::new(Some(tmp_dir.clone())).await?;
	let noctowl = Arc::new(RwLock::new(noctowl));

	{
		let noctowl = noctowl.write().await;

		let user_pid = "test_linear_regular_user";

		let (project_pid, _) = noctowl.create_project(&user_pid, "New Project").await?;
		let (doc_row, _) = noctowl.create_document(&user_pid, &project_pid, "Test Doc", None, None).await?;
		let doc_pid = doc_row.unwrap().doc_pid;

		let (_, (test_updates, _, _)) = get_test_updates(false, 15);

		for update in test_updates {
			noctowl.update_document(
				&user_pid,
				&project_pid,
				&doc_pid,
				update,
			).await?;
		}

		let doc = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?.0;
		let doc = doc.unwrap();

		let expected_output = format!(
			"({}: {{root: |line_0|line_1|line_2|line_3|line_4|line_5|line_6|line_7|line_8|line_9|line_10|line_11|line_12|line_13|line_14}})",
			doc_pid
		);

		let actual_output = doc.lock().await.to_string();

		assert_eq!(expected_output, actual_output);
	}

	noctowl.write().await.clean_up().await;

	fs::remove_dir_all(&tmp_dir).await.ok();

	Ok(())
}

#[tokio::test]
async fn test_multithreading_regular() -> Result<(), NoctowlError> {
	let tmp_dir = ".test_storage/test_multithreading_regular".to_string();
	let noctowl = Noctowl::new(Some(tmp_dir.clone())).await?;
	let noctowl = Arc::new(RwLock::new(noctowl));

	{
		let noctowl = noctowl.write().await;

		let user_pid = get_new_user_pid();

		let (project_pid, _) = noctowl.create_project(&user_pid, "New Project").await?;
		let (doc_row, _) = noctowl.create_document(&user_pid, &project_pid, "Test Doc", None, None).await?;
		let doc_pid = doc_row.unwrap().doc_pid;

		let (_, (test_updates, _, _)) = get_test_updates(false, 15);
		let test_update_scenarios = true;

		if test_update_scenarios {
			let u0 = test_updates.get(0).unwrap().clone();
			let u1 = test_updates.get(1).unwrap().clone();
			let u2 = test_updates.get(2).unwrap().clone();
			let _u3 = test_updates.get(3).unwrap().clone();
			let u4 = test_updates.get(4).unwrap().clone();
			let _u5 = test_updates.get(5).unwrap().clone();
			let merged_updates = merge_updates_v1(&vec![u0.as_slice(), u1.as_slice()]).unwrap();
			let merged_updates2 = merge_updates_v1(&vec![u2.as_slice(), u1.as_slice(), u0.as_slice()]).unwrap();
			let wrong_data_update = "this data is wrong".as_bytes().to_vec();

			// new update, should apply
			noctowl.update_document(&user_pid, &project_pid, &doc_pid, u0.clone()).await?;
			// new update, should apply
			noctowl.update_document(&user_pid, &project_pid, &doc_pid, u1.clone()).await?;

			// old updates, shouldn't apply but return OK
			noctowl.update_document(&user_pid, &project_pid, &doc_pid, merged_updates.clone()).await?;
			// old updates and new, should apply, return OK
			noctowl.update_document(&user_pid, &project_pid, &doc_pid, merged_updates2).await?;
			// wrong data, should gracefully fail, return Err
			match noctowl.update_document(&user_pid, &project_pid, &doc_pid, wrong_data_update).await {
				Ok(_) => panic!("should have failed"),
				Err(_) => {}
			}

			// missing prev update, shouldn't apply, should WAIT and retry
			noctowl.update_document(&user_pid, &project_pid, &doc_pid, u4).await?;
		}

		let mut tasks = Vec::new();

		for (i, update) in test_updates.into_iter().enumerate() {
			let delay = std::time::Duration::from_millis(i as u64 * 10);
			tokio::time::sleep(delay).await;

			let task = task::spawn({
				let noctowl = noctowl.clone();
				let user_pid = user_pid.clone();
				let project_pid = project_pid.clone();
				let doc_pid = doc_pid.clone();
				let update = update.clone();

				async move {
					noctowl.update_document(
						&user_pid,
						&project_pid,
						&doc_pid,
						update,
					).await.expect("Error processing update");
				}
			});

			tasks.push(task);
		}

		for task in tasks {
			task.await.unwrap();
		}

		let doc = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?.0;
		let doc = doc.unwrap();

		let expected_output = format!(
			"({}: {{root: |line_0|line_1|line_2|line_3|line_4|line_5|line_6|line_7|line_8|line_9|line_10|line_11|line_12|line_13|line_14}})",
			doc_pid
		);

		let actual_output = doc.lock().await.to_string();

		assert_eq!(expected_output, actual_output);
	}

	noctowl.write().await.clean_up().await;

	fs::remove_dir_all(&tmp_dir).await.ok();

	Ok(())
}

#[tokio::test]
// THIS TEST TAKES ~17 SECONDS TO RUN!
async fn test_with_real_doc() -> Result<(), NoctowlError> {
	let (base_doc, base_doc_updates) = get_base_doc().await?;

	let tmp_dir = ".test_storage/test_with_real_doc".to_string();
	let noctowl = Noctowl::new(Some(tmp_dir.clone())).await?;
	let noctowl = Arc::new(RwLock::new(noctowl));

	{
		let noctowl = noctowl.write().await;

		let user_pid = get_new_user_pid();

		let (project_pid, _) = noctowl.create_project(&user_pid, "New Project").await?;
		let (doc_row, _) = noctowl.create_document(&user_pid, &project_pid, "Test Doc", None, None).await?;
		let doc_pid = doc_row.unwrap().doc_pid;

		let mut tasks = Vec::new();

		for (i, update) in base_doc_updates.into_iter().enumerate() {
			let delay = std::time::Duration::from_millis(i as u64 * 1);
			tokio::time::sleep(delay).await;

			let task = task::spawn({
				let noctowl = noctowl.clone();
				let user_pid = user_pid.clone();
				let project_pid = project_pid.clone();
				let doc_pid = doc_pid.clone();
				let update = update.clone();

				async move {
					noctowl.update_document(
						&user_pid,
						&project_pid,
						&doc_pid,
						update.update_data,
					).await.expect("Error processing update");
				}
			});

			tasks.push(task);
		}

		for task in tasks {
			task.await.unwrap();
		}

		let doc = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?.0;
		let doc = doc.unwrap();

		let actual_output = format!("{:?}", doc.lock().await.get_doc_state());
		let expected_output = format!("{:?}", base_doc.lock().await.get_doc_state());

		assert_eq!(expected_output, actual_output);
	}

	noctowl.write().await.clean_up().await;

	fs::remove_dir_all(&tmp_dir).await.ok();

	Ok(())
}

#[tokio::test]
async fn yrs_basic_test() -> Result<(), NoctowlError> {
	let (updates, expected_doc_state) = get_test_updates_safe(4);

	let ydoc = yrs::Doc::with_options(Options::with_guid_and_client_id(
		Uuid::from("test"),
		0,
	));

	ydoc.get_or_insert_xml_text("root");

	{
		let update_index = 0;
		let tx = &mut ydoc.try_transact_mut().unwrap();
		let update_slice = updates[update_index].as_slice();
		let update = Update::decode_v1(update_slice).unwrap();
		tx.apply_update(update);
		let actual_state = ydoc.to_json(tx).to_string();
		assert_eq!(actual_state, expected_doc_state[update_index]);
	}

	// Yrs documentation says that updates without preceding updates will be kept and applied later
	// when the missing updates are received.
	// This is not the case (or i don't understand what missing update is, or even what an update is!)!
	{
		let tx = &mut ydoc.try_transact_mut().unwrap();
		let update = Update::decode_v1(updates[2].as_slice()).unwrap();
		tx.apply_update(update);
		let actual_state = ydoc.to_json(tx).to_string();
		assert_eq!(actual_state, expected_doc_state[0]);

		let update = Update::decode_v1(updates[1].as_slice()).unwrap();
		tx.apply_update(update);
		let actual_state = ydoc.to_json(tx).to_string();
		assert_eq!(actual_state, expected_doc_state[1]);
	}

	// redo update 2
	{
		let tx = &mut ydoc.try_transact_mut().unwrap();
		let update = Update::decode_v1(updates[2].as_slice()).unwrap();
		tx.apply_update(update);
		let actual_state = ydoc.to_json(tx).to_string();
		assert_eq!(actual_state, expected_doc_state[2]);
	}

	{
		let update_index = 3;
		let tx = &mut ydoc.try_transact_mut().unwrap();
		let update_slice = updates[update_index].as_slice();
		let update = Update::decode_v1(update_slice).unwrap();
		tx.apply_update(update);
		let actual_state = ydoc.to_json(tx).to_string();
		assert_eq!(actual_state, expected_doc_state[update_index]);
	}

	Ok(())
}

#[tokio::test]
async fn simple_simulation_test() -> Result<(), NoctowlError> {
	let tmp_dir = ".test_storage/simple_simulation_test".to_string();
	let noctowl = Noctowl::new(Some(tmp_dir.clone())).await?;
	let noctowl = Arc::new(RwLock::new(noctowl));

	{
		let noctowl = noctowl.write().await;

		let user_pid = get_new_user_pid();
		let project_pid = noctowl.create_project(&user_pid, "New Project").await?.0;
		let doc_pid = noctowl.create_document(&user_pid, &project_pid, "Test Doc", None, None)
			.await?.0.unwrap().doc_pid;

		let (expected_doc, (updates, expected_doc_state, _)) = get_test_updates(false, 6);

		{
			// the first update is send and handled by the server
			let update = updates[0].clone();

			noctowl.update_document(
				&user_pid,
				&project_pid,
				&doc_pid,
				update,
			).await?;

			let actual_doc_state = get_doc_json(&noctowl, &user_pid, &project_pid, &doc_pid).await;
			assert_eq!(actual_doc_state, format!("({}: {})", doc_pid, expected_doc_state[0]));
		}

		{
			// the second update, however, is not sent to the server
			// thus, it is added to the pending updates
			let second_update = updates[1].clone();

			// the third update, reaches the server though
			let third_update = updates[2].clone();

			match noctowl.update_document(
				&user_pid,
				&project_pid,
				&doc_pid,
				third_update.clone(),
			).await {
				Ok(YrsUpdateStatus::Pending) => println!("The update status is 'Pending' as expected"),
				Ok(status) => panic!("The update status should have been 'Pending', but was: {:?}", status),
				Err(e) => panic!("This shouldn't have failed: {}", e),
			}

			// the update is not applied, and says it's pending
			let actual_doc_state = get_doc_json(&noctowl, &user_pid, &project_pid, &doc_pid).await;
			assert_eq!(actual_doc_state, format!("({}: {})", doc_pid, expected_doc_state[0]));

			// then the client should merge their updates and try again
			let merged_updates = merge_updates_v1(&*vec![second_update.as_slice(), third_update.as_slice()]).unwrap();

			noctowl.update_document(
				&user_pid,
				&project_pid,
				&doc_pid,
				merged_updates.clone(),
			).await?;

			// thus, the update is finally applied
			let actual_doc_state = get_doc_json(&noctowl, &user_pid, &project_pid, &doc_pid).await;
			assert_eq!(actual_doc_state, format!("({}: {})", doc_pid, expected_doc_state[2]));
		}

		{
			// ...but there might be cases where the client keeps sending
			// out of order updates (there shouldn't be, but just in case of a bug or edge case)
			let _fourth_update = updates[3].clone();
			let fifth_update = updates[4].clone();
			let _sixth_update = updates[5].clone();

			match noctowl.update_document(
				&user_pid,
				&project_pid,
				&doc_pid,
				fifth_update.clone(),
			).await {
				Ok(YrsUpdateStatus::Pending) => println!("The update status is 'Pending' as expected"),
				Ok(status) => panic!("The update status should have been 'Pending', but was: {:?}", status),
				Err(e) => panic!("This shouldn't have failed: {}", e),
			}

			// let's say the client "lost" the fourth update
			// it won't be able to merge its updates, in that case, it will ask for a state vector to the server

			{
				let (doc, _) = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await.unwrap();
				let doc = doc.unwrap();
				let doc = doc.lock().await;

				let server_sv = doc.ydoc.transact().state_vector();

				println!("server_sv: {:?}", server_sv.encode_v1());

				// the clients returns the diff between the server state vector and its own
				let diff_update = {
					let merged_updates = merge_updates_v1(
						//updates.iter().take(updates.len() - 2).map(|u| u.as_slice()).collect::<Vec<_>>().as_slice()
						updates.iter().map(|u| u.as_slice()).collect::<Vec<_>>().as_slice()
					).unwrap();

					let update = Update::decode_v1(merged_updates.as_slice()).unwrap();
					let tmp = yrs::Doc::new();
					tmp.get_or_insert_xml_text("root");
					let mut tx = tmp.transact_mut();
					tx.apply_update(update);
					tx.encode_diff_v1(&server_sv)
				};

				drop(doc);

				/*println!("fourth_update: {:?}", fourth_update);
				println!("fifth_update: {:?}", fifth_update);
				println!("sixth_update: {:?}", sixth_update);*/
				println!("diff_update: {:?}", diff_update);

				// the server receives the diff, and applies it successfully
				match noctowl.update_document(
					&user_pid,
					&project_pid,
					&doc_pid,
					diff_update,
				).await {
					Ok(YrsUpdateStatus::Updated) => println!("The update status is 'Updated' as expected"),
					Ok(status) => panic!("The update status should have been 'Updated', but was: {:?}", status),
					Err(e) => panic!("This shouldn't have failed: {}", e),
				}
			}

			let actual_doc_state = get_doc_json(&noctowl, &user_pid, &project_pid, &doc_pid).await;
			assert_eq!(actual_doc_state, format!("({}: {})", doc_pid, expected_doc_state[5]));
		}

		// final assertion
		{
			let actual_doc = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?.0;
			let actual_doc = actual_doc.unwrap();

			let actual_output = format!("{:?}", actual_doc.lock().await.get_doc_state());
			let expected_output = format!("{:?}", expected_doc.get_doc_state());

			assert_eq!(actual_output, expected_output);
		}
	}

	noctowl.write().await.clean_up().await;

	fs::remove_dir_all(&tmp_dir).await.ok();

	Ok(())
}
