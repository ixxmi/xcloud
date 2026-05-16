# xcloud

`xcloud` is a small central file sync MVP written in Go. One process runs as the
server and stores metadata plus content-addressed chunks. Any number of clients
sync a configured local directory with that server.

This first version focuses on a safe, understandable core:

- HTTP API with optional Bearer token authentication.
- SHA-256 content hashes for every chunk and full file.
- Chunk upload/download with server-side hash verification.
- Server-side version metadata and delete tombstones.
- Client-side state file for idempotent scan-based sync.
- Conflict files instead of blind overwrites.
- Atomic file writes on download.
- Symlinks, special files, and `.xcloud` state directories are skipped.

## Build

```sh
go build ./cmd/xcloud
```

## Start Server

```sh
XCLOUD_TOKEN=secret ./xcloud server -addr :8080 -data ./server-data
```

The server stores metadata in `server-data/metadata.json` and chunks under
`server-data/chunks`.

## Start Clients

Terminal 1:

```sh
mkdir -p /tmp/xcloud-a
XCLOUD_TOKEN=secret ./xcloud client -root /tmp/xcloud-a -server http://127.0.0.1:8080 -device laptop-a
```

Terminal 2:

```sh
mkdir -p /tmp/xcloud-b
XCLOUD_TOKEN=secret ./xcloud client -root /tmp/xcloud-b -server http://127.0.0.1:8080 -device laptop-b
```

Create or modify files in either directory. The next scan cycle uploads the
change to the server and the other client downloads it.

For a single sync cycle:

```sh
XCLOUD_TOKEN=secret ./xcloud client -root /tmp/xcloud-a -server http://127.0.0.1:8080 -device laptop-a -once
```

Local deletes are conservative by default. To propagate local deletes to the
server, run the client with:

```sh
-delete-remote
```

## Security Notes

This is an MVP, not a complete hardened cloud product. For production use, add:

- TLS 1.3 or mTLS in front of the HTTP server.
- Per-device registration and revocation instead of one shared token.
- PostgreSQL or another transactional metadata store.
- Object storage such as MinIO/S3 for chunk data.
- End-to-end encryption for chunk bytes and optionally encrypted paths.
- API rate limits, audit logs, retention policy, and snapshot rollback.

