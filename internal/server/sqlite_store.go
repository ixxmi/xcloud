package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"xcloud/internal/syncmodel"
)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS server_config (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  domain TEXT NOT NULL,
  port INTEGER NOT NULL,
  data_dir TEXT NOT NULL,
  listen_host TEXT NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  email TEXT NOT NULL DEFAULT '',
  password_hash TEXT NOT NULL,
  sync_token_hash TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  disabled INTEGER NOT NULL DEFAULT 0,
  sync_enabled INTEGER NOT NULL DEFAULT 0,
  sync_settings_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS client_tokens (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  device_id TEXT NOT NULL DEFAULT '',
  hostname TEXT NOT NULL DEFAULT '',
  token_hash TEXT NOT NULL,
  disabled INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  last_used_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS client_devices (
  account_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  hostname TEXT NOT NULL DEFAULT '',
  storage_root TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (account_id, device_id)
);

CREATE TABLE IF NOT EXISTS spaces (
  account_id TEXT NOT NULL,
  id TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_id, id)
);
`

func openSQLite(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func closeSQLite(db *sql.DB) {
	if db != nil {
		_ = db.Close()
	}
}

func sqliteHasControlData(db *sql.DB) (bool, error) {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounts`).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func loadControlStateFromSQLite(db *sql.DB, state *syncmodel.ServerState) error {
	accounts := map[string]*syncmodel.Account{}
	rows, err := db.Query(`SELECT id, username, display_name, email, password_hash, sync_token_hash, is_admin, disabled, sync_enabled, sync_settings_json, created_at, updated_at FROM accounts`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		account := &syncmodel.Account{}
		var isAdmin, disabled, syncEnabled int
		var settingsJSON string
		if err := rows.Scan(&account.ID, &account.Username, &account.DisplayName, &account.Email, &account.PasswordHash, &account.SyncTokenHash, &isAdmin, &disabled, &syncEnabled, &settingsJSON, &account.CreatedAt, &account.UpdatedAt); err != nil {
			return err
		}
		account.IsAdmin = isAdmin != 0
		account.Disabled = disabled != 0
		account.SyncEnabled = syncEnabled != 0
		if settingsJSON != "" {
			_ = json.Unmarshal([]byte(settingsJSON), &account.SyncSettings)
		}
		account.SyncSettings = syncmodel.NormalizeSyncSettings(account.SyncSettings)
		accounts[account.ID] = account
	}
	if err := rows.Err(); err != nil {
		return err
	}
	state.Accounts = accounts

	tokens := map[string]*syncmodel.ClientToken{}
	rows, err = db.Query(`SELECT id, account_id, device_id, hostname, token_hash, disabled, created_at, last_used_at FROM client_tokens`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		token := &syncmodel.ClientToken{}
		var disabled int
		if err := rows.Scan(&token.ID, &token.AccountID, &token.DeviceID, &token.Hostname, &token.TokenHash, &disabled, &token.CreatedAt, &token.LastUsedAt); err != nil {
			return err
		}
		token.Disabled = disabled != 0
		tokens[token.ID] = token
	}
	if err := rows.Err(); err != nil {
		return err
	}
	state.ClientTokens = tokens

	devices := map[string]*syncmodel.ClientDevice{}
	rows, err = db.Query(`SELECT account_id, device_id, hostname, storage_root, created_at, updated_at, last_seen_at FROM client_devices`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		device := &syncmodel.ClientDevice{}
		if err := rows.Scan(&device.AccountID, &device.DeviceID, &device.Hostname, &device.StorageRoot, &device.CreatedAt, &device.UpdatedAt, &device.LastSeenAt); err != nil {
			return err
		}
		devices[deviceKey(device.AccountID, device.DeviceID)] = device
	}
	if err := rows.Err(); err != nil {
		return err
	}
	state.ClientDevices = devices

	spaces := map[string]*syncmodel.SyncSpace{}
	rows, err = db.Query(`SELECT account_id, id, name, description, active, created_at, updated_at FROM spaces`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		space := &syncmodel.SyncSpace{}
		var active int
		if err := rows.Scan(&space.AccountID, &space.ID, &space.Name, &space.Description, &active, &space.CreatedAt, &space.UpdatedAt); err != nil {
			return err
		}
		space.Active = active != 0
		spaces[spaceKey(space.AccountID, space.ID)] = space
	}
	if err := rows.Err(); err != nil {
		return err
	}
	state.Spaces = spaces
	return nil
}

func saveControlStateToSQLite(db *sql.DB, state syncmodel.ServerState) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	for _, stmt := range []string{
		`DELETE FROM accounts`,
		`DELETE FROM client_tokens`,
		`DELETE FROM client_devices`,
		`DELETE FROM spaces`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	for _, account := range state.Accounts {
		settingsJSON, err := json.Marshal(syncmodel.NormalizeSyncSettings(account.SyncSettings))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO accounts (id, username, display_name, email, password_hash, sync_token_hash, is_admin, disabled, sync_enabled, sync_settings_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			account.ID, account.Username, account.DisplayName, account.Email, account.PasswordHash, account.SyncTokenHash, boolInt(account.IsAdmin), boolInt(account.Disabled), boolInt(account.SyncEnabled), string(settingsJSON), account.CreatedAt, account.UpdatedAt); err != nil {
			return err
		}
	}
	for _, token := range state.ClientTokens {
		if _, err := tx.Exec(`INSERT INTO client_tokens (id, account_id, device_id, hostname, token_hash, disabled, created_at, last_used_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			token.ID, token.AccountID, token.DeviceID, token.Hostname, token.TokenHash, boolInt(token.Disabled), token.CreatedAt, token.LastUsedAt); err != nil {
			return err
		}
	}
	for _, device := range state.ClientDevices {
		if _, err := tx.Exec(`INSERT INTO client_devices (account_id, device_id, hostname, storage_root, created_at, updated_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			device.AccountID, device.DeviceID, device.Hostname, device.StorageRoot, device.CreatedAt, device.UpdatedAt, device.LastSeenAt); err != nil {
			return err
		}
	}
	for _, space := range state.Spaces {
		if _, err := tx.Exec(`INSERT INTO spaces (account_id, id, name, description, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			space.AccountID, space.ID, space.Name, space.Description, boolInt(space.Active), space.CreatedAt, space.UpdatedAt); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

func loadRuntimeConfigFromSQLite(db *sql.DB, fallback RuntimeConfig) (RuntimeConfig, error) {
	cfg := fallback
	var listenHost string
	err := db.QueryRow(`SELECT domain, port, data_dir, listen_host FROM server_config WHERE id = 1`).Scan(&cfg.Domain, &cfg.Port, &cfg.DataDir, &listenHost)
	if errors.Is(err, sql.ErrNoRows) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	cfg.ListenHost = listenHost
	cfg.Normalize()
	return cfg, nil
}

func saveRuntimeConfigToSQLite(db *sql.DB, cfg RuntimeConfig) error {
	cfg.Normalize()
	_, err := db.Exec(`INSERT INTO server_config (id, domain, port, data_dir, listen_host, updated_at) VALUES (1, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET domain = excluded.domain, port = excluded.port, data_dir = excluded.data_dir, listen_host = excluded.listen_host, updated_at = excluded.updated_at`,
		cfg.Domain, cfg.Port, cfg.DataDir, cfg.ListenHost, time.Now().Unix())
	return err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
