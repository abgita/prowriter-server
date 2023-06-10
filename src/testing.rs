use std::sync::Arc;
use tokio::{task};
use tokio::sync::RwLock;

mod common;
mod noctowl;
use crate::common::{logging};
use crate::common::utils::get_new_user_pid;
use crate::noctowl::{NoctowlError};
use crate::noctowl::lib::Noctowl;
use crate::noctowl::testing::{get_test_updates, get_test_users};

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
	let mut noctowl = noctowl.write().await;

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

	let test_updates = get_test_updates(false, 15);
	let multithreaded = false;

	if multithreaded {
		let mut tasks = Vec::new();

		for (i, update) in test_updates.into_iter().enumerate() {
			let delay = std::time::Duration::from_millis(i as u64 * 10);
			tokio::time::sleep(delay).await;

			let task = task::spawn({
				let mut noctowl = noctowl.clone();
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
