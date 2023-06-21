use std::convert::Infallible;
use std::sync::Arc;

use warp::{Filter, Rejection, Reply};

use crate::accounts::{jwt};
use crate::api::controllers::{create_document, create_folder, create_project, get_document, get_project, get_projects, update_document};
use crate::api::controllers::get_document::GetDocumentParams;
use crate::noctowl::lib::Noctowl;

fn with_noctowl(
	noctowl: Arc<Noctowl>,
) -> impl Filter<Extract=(Arc<Noctowl>, ), Error=Infallible> + Clone {
	warp::any().map(move || noctowl.clone())
}

pub fn get_routes(
	noctowl: &Arc<Noctowl>
) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
	// projects/create
	let create_project = warp::path!("create")
		.and(warp::post())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and(warp::body::json())
		.and_then(create_project);

	// projects/{project_pid}/folder/create
	let create_folder = warp::path!(String / "folder" / "create")
		.and(warp::post())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and(warp::body::json())
		.and_then(create_folder);

	// projects/{project_pid}/docs/create
	let create_document = warp::path!(String / "docs" / "create")
		.and(warp::post())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and(warp::body::json())
		.and_then(create_document);

	// projects/{project_pid}/docs/{doc_pid}
	let get_document = warp::path!(String / "docs" / String)
		.and(warp::post())
		.and(warp::query::<GetDocumentParams>())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and_then(get_document);

	let update_document = warp::path!(String / "docs" / String / "update")
		.and(warp::put())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and(warp::header::exact("content-type", "application/octet-stream"))
		.and(warp::body::content_length_limit(10 * 1024 * 1024))
		.and(warp::body::bytes())
		.and_then(update_document);

	// projects/{project_pid}
	let get_project = warp::path!(String)
		.and(warp::post())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and_then(get_project);

	// user/projects
	let get_projects = warp::path("projects")
		.and(warp::post())
		.and(jwt::jwt_auth_filter())
		.and(with_noctowl(noctowl.clone()))
		.and_then(get_projects);

	warp::any()
		.and(warp::path("projects")
			.and(create_project
				.or(create_folder)
				.or(create_document)
				.or(get_document)
				.or(update_document)
				.or(get_project))
		.or(warp::path("users")
			.and(get_projects)))
}
