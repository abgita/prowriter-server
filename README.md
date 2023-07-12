# Prowriter server

REST server for Prowriter's projects and documents.

## How it works

It receives requests under `/v1` and passes them to Noctowl.
Noctowl is the part that actually manages projects, folders and documents.

It uses [Yrs](https://github.com/y-crdt/y-crdt) to keep a document's CRDT state, apply updates and create snapshots. Then it
persists the document data in SQLite, so a document can be loaded again or
restored to an earlier version.

<pre>
Client
  |
  | HTTP requests (JSON and binary document updates)
  v
RESTful server (Warp + Tokio)
  |
  | project, folder and document operations
  v
Noctowl
  ├──> Yrs ─────────────────> document state, updates and snapshots
  |
  ├──> in-memory cache ─────> active documents and database connections
  |
  └──> SQLite data store ───> .storage/
                               - main.sqlite: project metadata
                               - project/document databases: content and history
</pre>

The document cache and the database connection pools are kept in memory while
they are in use. Stale connections are cleaned up periodically.

## Project files

<pre>
.
├── src
│   ├── api ────────────────> REST routes and their controllers
│   ├── noctowl ────────────> Projects, folders, documents, Yrs and persistence
│   ├── common ─────────────> Logging and general utilities
│   └── main.rs ────────────> Starts the server and background cleanup task
├── docs ───────────────────> Stored document data
└── .storage ───────────────> SQLite data created at runtime
</pre>

## Running locally

```bash
cargo run
```

The development server listens on `0.0.0.0:3003`.
