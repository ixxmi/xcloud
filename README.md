# xcloud

`xcloud` is a central file-sync MVP written in Go. One process runs as the
server and stores metadata plus content-addressed chunks. Clients sync only the
files placed under their local `xcloud` storage root, grouped by Space.

中文详细文档见 [docs/中文详细文档.md](docs/中文详细文档.md)。

## Current Sync Model

- Each account has isolated data.
- Each account can have multiple Spaces.
- Each active Space maps to one local directory:
  - default Space: `xcloud/default`
  - other Space: `xcloud/<space-id>`
- Clients no longer report arbitrary local folders for selection.
- New files, edits, and deletes under `xcloud/<space-id>` are synced to every
  logged-in client under the same account and Space.
- Local deletes are always propagated.
- Deleted files are retained in the server trash for 10 days and can be restored
  from `/admin`; restored files sync back to all clients.
- The management console can change each client device's global xcloud storage
  root. This is device-level configuration, not per-folder configuration.

The client skips only its internal `.xcloud` state directory, symlinks, and
non-regular special files.

## Build

```sh
go build -o xcloud ./cmd/xcloud
```

If your Go build cache is not writable:

```sh
GOCACHE=/tmp/go-build-cache go build -o xcloud ./cmd/xcloud
```

## Start Server

```sh
./xcloud server -addr :8080 -data ./server-data
```

Open the management console:

```text
http://127.0.0.1:8080/admin
```

The server stores metadata in `server-data/metadata.json` and chunks under
`server-data/chunks`.

## Start Client Without Token

```sh
mkdir -p ./xcloud
./xcloud client -server http://127.0.0.1:8080
```

Open the local client console:

```text
http://127.0.0.1:18080
```

Log in with the cloud account and click `开启此账号同步`. This is an account-level
switch: when any logged-in client enables sync, every other logged-in client for
that account detects it and starts syncing.

Then put files under:

```text
./xcloud/default/
```

For a custom client storage root:

```sh
mkdir -p /data/xcloud
./xcloud client -root /data/xcloud -server http://127.0.0.1:8080
```

## Token-Based Startup

Legacy token-based startup is still supported for scripts and services:

```sh
mkdir -p /tmp/xcloud-a/default /tmp/xcloud-b/default

./xcloud client \
  -root /tmp/xcloud-a \
  -server http://127.0.0.1:8080 \
  -token <account-token> \
  -device laptop-a

./xcloud client \
  -root /tmp/xcloud-b \
  -server http://127.0.0.1:8080 \
  -token <same-account-token> \
  -device laptop-b
```

Everything under `/tmp/xcloud-a/default` syncs with `/tmp/xcloud-b/default`.
If the account has another active Space such as `docs`, clients also sync
`/tmp/xcloud-a/docs` with `/tmp/xcloud-b/docs`.

For one sync cycle:

```sh
./xcloud client \
  -root /tmp/xcloud-a \
  -server http://127.0.0.1:8080 \
  -token <account-token> \
  -device laptop-a \
  -once
```

## Client Flags

```text
-root           client xcloud storage root; defaults to <working-directory>/xcloud
-server         server URL, default http://127.0.0.1:8080
-token          optional account sync token; if omitted, starts local client console
-client-addr    local client console address, default 127.0.0.1:18080
-client-config  local client config path, default ~/.xcloud/client-config.json
-space          fallback Space ID, default default; active Spaces are loaded from server
-device         device ID; defaults to hostname plus storage-root fingerprint
-state          supervisor state file, default ~/.xcloud/discovery-state.json
-interval       fallback scan interval, default 10s
-chunk-size     chunk size, default 4 MiB
-once           run one sync cycle and exit
-delete-remote  compatibility flag; local deletes are always propagated
```

## Admin Console

The `/admin` console supports:

- login and registration;
- account management for admin users;
- Space creation and enable/disable;
- device-level xcloud storage-root configuration;
- account-level sync trigger settings;
- sync records for uploads, downloads, deletes, conflicts, skips, and failures;
- cloud trash with 10-day retention and restore.

## Security Notes

This is an MVP, not a hardened production cloud product. For production use,
add TLS, device revocation, persistent sessions, CSRF protection, rate limits,
audit logs, PostgreSQL metadata storage, object storage, and optional
end-to-end encryption.
