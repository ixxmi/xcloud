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
- Clients automatically retry transient network failures, HTTP `429`, and
  server `5xx` responses. If the network is down for longer than the retry
  window, the client keeps running and resumes from server events plus local
  scans after the next successful connection.

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
./xcloud server
```

Open the management console:

```text
http://ixxmi.com:18002/admin
```

The server stores metadata in `server-data/metadata.json` and chunks under
`server-data/chunks`. Control-plane data is also mirrored into SQLite at
`server-data/server.db`: server config, accounts, client tokens, devices,
Spaces, and sync settings.

Server defaults are loaded from `xcloud-server.json`:

```json
{
  "domain": "ixxmi.com",
  "port": 18002,
  "data_dir": "server-data"
}
```

Admins can update the public domain, port, and data directory in `/admin` under
`服务配置`. Port and data directory changes take effect after restarting the
server process.

## Start Client Without Token

```sh
mkdir -p ./xcloud
./xcloud client
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
./xcloud client -root /data/xcloud
```

## Watchdog Startup

The watchdog script builds separate process binaries so the running process
names are easy to manage:

```sh
./scripts/xcloud-watchdog.sh server
./scripts/xcloud-watchdog.sh client
```

- server process binary: `xcloud-runtime/bin/xclouds`
- client process binary: `xcloud-runtime/bin/xcloudc`
- logs: `xcloud-runtime/logs/xclouds.log` and `xcloud-runtime/logs/xcloudc.log`
- lock dirs: `xcloud-runtime/run/xclouds.lock` and `xcloud-runtime/run/xcloudc.lock`

The watchdog restarts the child process after an unexpected exit. Extra CLI
arguments are passed through, for example:

```sh
./scripts/xcloud-watchdog.sh client -root /data/xcloud -server http://ixxmi.com:18002
```

## Token-Based Startup

Legacy token-based startup is still supported for scripts and services:

```sh
mkdir -p /tmp/xcloud-a/default /tmp/xcloud-b/default

./xcloud client \
  -root /tmp/xcloud-a \
  -token <account-token> \
  -device laptop-a

./xcloud client \
  -root /tmp/xcloud-b \
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
  -token <account-token> \
  -device laptop-a \
  -once
```

## Client Flags

```text
-root           client xcloud storage root; defaults to <working-directory>/xcloud
-server         server URL, default http://ixxmi.com:18002
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
- server public domain, port, and data directory configuration for admins;
- sync records for uploads, downloads, deletes, conflicts, skips, and failures;
- cloud trash with 10-day retention and restore.

## Reconnect Behavior

The client uses HTTP polling plus filesystem watching rather than a permanent
socket connection. Each sync API call is retried with exponential backoff for
temporary network errors, timeouts, HTTP `429`, and `5xx` responses. Permanent
client-side errors such as `400`, `401`, `403`, and `404` fail fast.

When all retry attempts are exhausted, the current sync pass fails but the
client process does not exit. Realtime watching continues when available, and
the fallback scan interval keeps running. Once the server is reachable again,
the client continues pulling server events from the last saved sequence and
rescans local `xcloud/<space-id>` directories, so offline edits, creates, and
deletes are picked up in the next successful pass.

## Security Notes

This is an MVP, not a hardened production cloud product. For production use,
add TLS, device revocation, persistent sessions, CSRF protection, rate limits,
audit logs, PostgreSQL metadata storage, object storage, and optional
end-to-end encryption.
