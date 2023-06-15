use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::task;
use yrs::merge_updates_v1;

use crate::common::logging;
use crate::common::utils::get_new_user_pid;
use crate::noctowl::lib::Noctowl;
use crate::noctowl::NoctowlError;
use crate::noctowl::testing::{get_test_updates, get_test_users};

mod common;
mod noctowl;
// -------------------------------------------------------------------------------------------------

#[tokio::main]
pub async fn main() -> Result<(), NoctowlError> {
	logging::setup(false);

	let noctowl = Noctowl::new(Some(".test_storage".to_string())).await?;
	let noctowl = Arc::new(RwLock::new(noctowl));

	//heavy_test(&noctowl).await?;
	regular_test(&noctowl).await?;

	noctowl.write().await.clean_up().await;

	Ok(())
}

async fn heavy_test(noctowl: &Arc<RwLock<Noctowl>>) -> Result<(), NoctowlError> {
	let users = get_test_users(noctowl, 50).await?;

	Ok(())
}

async fn regular_test(noctowl: &Arc<RwLock<Noctowl>>) -> Result<(), NoctowlError> {
	let noctowl = noctowl.write().await;

	// this should happen when the user signs up
	let user_pid = get_new_user_pid();

	clog!("POST:api/v1/projects/create");
	let (project_pid, _) = noctowl.create_project(&user_pid, "New Project").await?;

	clog!("POST:api/v1/projects/{}/docs/create", &project_pid);
	let (doc_row, _) = noctowl.create_document(&user_pid, &project_pid, "Test Doc", None, None).await?;
	let doc_pid = doc_row.unwrap().doc_pid;

	{
		clog!("GET:api/v1/projects/{}/docs/{}", &project_pid, &doc_pid);
		let (doc, _) = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?;

		let doc = doc.unwrap();
		clog!("Doc: {}", doc.lock().await)
	}

	let test_updates = get_test_updates(true, 15);
	let multithreaded = true;
	let test_update_scenarios = false;

	if test_update_scenarios {
		let u0 = test_updates.get(0).unwrap().clone();
		let u1 = test_updates.get(1).unwrap().clone();
		let u2 = test_updates.get(2).unwrap().clone();
		let u3 = test_updates.get(3).unwrap().clone();
		let u4 = test_updates.get(4).unwrap().clone();
		let u5 = test_updates.get(5).unwrap().clone();
		let merged_updates = merge_updates_v1(&vec![u0.as_slice(), u1.as_slice()]).unwrap();
		let merged_updates2 = merge_updates_v1(&vec![u2.as_slice(), u1.as_slice(), u0.as_slice()]).unwrap();
		let wrong_data_update = "this data is wrong".as_bytes().to_vec();

		slog!("--------------------------------------------------------------------------------------------------------- START");
		slog!("--------------------------------------------------------------------------------------------- new update, should apply ");
		noctowl.update_document(&user_pid, &project_pid, &doc_pid, u0.clone()).await?;
		slog!("--------------------------------------------------------------------------------------------- new update, should apply ");
		noctowl.update_document(&user_pid, &project_pid, &doc_pid, u1.clone()).await?;

		slog!("-------------------------------------------------------------------------------- old updates, shouldn't apply but return OK");
		noctowl.update_document(&user_pid, &project_pid, &doc_pid, merged_updates.clone()).await?;
		slog!("-------------------------------------------------------------------------------- old updates and new, should apply, return OK");
		noctowl.update_document(&user_pid, &project_pid, &doc_pid, merged_updates2).await?;
		slog!("-------------------------------------------------------------------------------- wrong data, should gracefully fail, return Err");
		noctowl.update_document(&user_pid, &project_pid, &doc_pid, wrong_data_update).await?;

		slog!("---------------------------------------------------------------- missing prev update, shouldn't apply, should WAIT and retry");
		noctowl.update_document(&user_pid, &project_pid, &doc_pid, u4).await?;
		slog!("--------------------------------------------------------------------------------------------------------- __END");
	}

	if multithreaded {
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
	} else {
		for update in test_updates {
			noctowl.update_document(
				&user_pid,
				&project_pid,
				&doc_pid,
				update,
			).await?;
		}
	}

	let doc = noctowl.get_document(&user_pid, &project_pid, &doc_pid, None, None).await?.0;
	let doc = doc.unwrap();

	clog!("Final doc: {}", doc.lock().await);

	Ok(())
}
