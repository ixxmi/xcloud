package server

import (
	"path/filepath"
	"testing"

	"xcloud/internal/syncmodel"
)

func TestStorePersistsControlDataToSQLite(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	account, _, err := store.CreateAccount("sqlite-user", "sqlite@example.com", "SQLite User", "password123", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetAccountSyncEnabled(account.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSyncSettings(account.ID, syncmodel.SyncSettings{RealtimeEnabled: true, DebounceMillis: 123, IntervalSeconds: 7}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSpaceActive(account.ID, "default", false); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	loaded, ok := reopened.AccountByUsername("sqlite-user")
	if !ok {
		t.Fatal("account not loaded from sqlite")
	}
	if !loaded.SyncEnabled {
		t.Fatal("SyncEnabled = false, want true")
	}
	if loaded.SyncSettings.DebounceMillis != 123 || loaded.SyncSettings.IntervalSeconds != 7 {
		t.Fatalf("SyncSettings = %+v", loaded.SyncSettings)
	}
	spaces := reopened.ListSpaces(account.ID)
	if len(spaces) != 1 {
		t.Fatalf("spaces len = %d, want 1", len(spaces))
	}
	if spaces[0].Space.Active {
		t.Fatal("default space active = true, want false")
	}
}

func TestRuntimeConfigPersistsToSQLite(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	cfg := RuntimeConfig{
		Path:       filepath.Join(root, "xcloud-server.json"),
		Domain:     "db.example.com",
		Port:       18004,
		DataDir:    root,
		ListenHost: "127.0.0.1",
	}
	if err := store.SaveRuntimeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.RuntimeConfig(DefaultRuntimeConfig(cfg.Path))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Domain != "db.example.com" || loaded.Port != 18004 || loaded.ListenHost != "127.0.0.1" {
		t.Fatalf("loaded config = %+v", loaded)
	}
}
