# xcloud

`xcloud` is a small central file sync MVP written in Go. One process runs as the
server and stores metadata plus content-addressed chunks. Any number of clients
sync a configured local directory with that server.

中文详细文档见 [docs/中文详细文档.md](docs/中文详细文档.md)。

The server also includes a management console at `/admin`. Users can register
from the login page, and administrators can manage accounts, passwords, sync
tokens, reported client folders, and per-account sync spaces. A client first
reports its local folder to the gateway; the folder starts syncing only after it
is selected into a sync space in the console. Files only sync inside the same
account and same selected sync space.

This first version focuses on a safe, understandable core:

- HTTP API with optional Bearer token authentication.
- Per-account sync tokens and per-account sync spaces.
- Management console for login, registration, accounts, passwords, reported
  client folders, sync spaces, and token rotation.
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
./xcloud server -addr :8080 -data ./server-data
```

The server stores metadata in `server-data/metadata.json` and chunks under
`server-data/chunks`.

Open `http://127.0.0.1:8080/admin`, then log in or register a normal account
from the same page. Reset each account's sync token before configuring clients.
The token is shown only once.

## Start Clients

Terminal 1:

```sh
mkdir -p /tmp/xcloud-a
./xcloud client -root /tmp/xcloud-a -server http://127.0.0.1:8080 -token <account-token> -space default -device laptop-a
```

Terminal 2:

```sh
mkdir -p /tmp/xcloud-b
./xcloud client -root /tmp/xcloud-b -server http://127.0.0.1:8080 -token <same-account-token> -space default -device laptop-b
```

After the first run, open `/admin`, go to `目录与 Space`, and select each
reported client folder into the same Space. Before selection the client only
reports its folder and waits; it will not upload or download files. Once two or
more folders under the same account are selected into the same Space, changes in
either directory sync through the server. Clients using another account token or
folders selected into another Space do not see these files.

For a single sync cycle:

```sh
./xcloud client -root /tmp/xcloud-a -server http://127.0.0.1:8080 -token <account-token> -space default -device laptop-a -once
```

`-space` is a suggested Space for the folder report. The effective Space is the
one selected by the gateway in the management console.

`-root` is optional. Without it, the client first reports only top-level roots
such as the current working directory and the user's home directory. In the
console, use `展开下一级` to ask the client to report the next level. Already
reported folders are cached locally and are not reported repeatedly. With
`-root`, the client runs in compatibility mode for one explicit folder.

Local deletes are conservative by default. To propagate local deletes to the
server, run the client with:

```sh
-delete-remote
```

## Security Notes

This is an MVP, not a complete hardened cloud product. For production use, add:

- TLS 1.3 or mTLS in front of the HTTP server.
- Per-device registration and revocation in addition to account tokens.
- PostgreSQL or another transactional metadata store.
- Object storage such as MinIO/S3 for chunk data.
- End-to-end encryption for chunk bytes and optionally encrypted paths.
- API rate limits, audit logs, retention policy, and snapshot rollback.
