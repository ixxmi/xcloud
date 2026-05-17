package client

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"xcloud/internal/server"
	"xcloud/internal/syncmodel"
)

func TestBidirectionalSyncAppliesRemoteEditsBackToOrigin(t *testing.T) {
	t.Parallel()

	store, err := server.NewStore(filepath.Join(t.TempDir(), "server"))
	if err != nil {
		t.Fatal(err)
	}
	account, token, err := store.CreateAccount("sync-user", "", "", "password123", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetAccountSyncEnabled(account.ID, true); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(store, "", slog.Default()).Handler())
	defer srv.Close()

	rootA := filepath.Join(t.TempDir(), "client-a")
	rootB := filepath.Join(t.TempDir(), "client-b")
	if err := os.MkdirAll(rootA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootB, 0o755); err != nil {
		t.Fatal(err)
	}

	engineA := newTestEngine(t, srv.URL, token, "dev-a", rootA)
	engineB := newTestEngine(t, srv.URL, token, "dev-b", rootB)
	ctx := context.Background()

	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, filepath.Join(engineA.cfg.LocalRoot, "note.txt"), "from a")
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(engineB.cfg.LocalRoot, "note.txt"), "from a")

	writeTestFile(t, filepath.Join(engineB.cfg.LocalRoot, "note.txt"), "from b")
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(engineA.cfg.LocalRoot, "note.txt"), "from b")
}

func TestDeleteCreatesTrashAndRestoreSyncsToAllClients(t *testing.T) {
	t.Parallel()

	store, err := server.NewStore(filepath.Join(t.TempDir(), "server"))
	if err != nil {
		t.Fatal(err)
	}
	account, token, err := store.CreateAccount("trash-user", "", "", "password123", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetAccountSyncEnabled(account.ID, true); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(store, "", slog.Default()).Handler())
	defer srv.Close()

	engineA := newTestEngine(t, srv.URL, token, "trash-a", filepath.Join(t.TempDir(), "client-a"))
	engineB := newTestEngine(t, srv.URL, token, "trash-b", filepath.Join(t.TempDir(), "client-b"))
	ctx := context.Background()

	writeTestFile(t, filepath.Join(engineA.cfg.LocalRoot, "doc.txt"), "keep me")
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(engineB.cfg.LocalRoot, "doc.txt"), "keep me")

	if err := os.Remove(filepath.Join(engineA.cfg.LocalRoot, "doc.txt")); err != nil {
		t.Fatal(err)
	}
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileMissing(t, filepath.Join(engineB.cfg.LocalRoot, "doc.txt"))

	trash := store.ListTrash(account.ID, 10)
	if len(trash) != 1 {
		t.Fatalf("trash length = %d, want 1", len(trash))
	}
	if trash[0].Path != "doc.txt" || trash[0].Size != int64(len("keep me")) {
		t.Fatalf("trash entry = %+v", trash[0])
	}

	_, err = store.RestoreTrash(account.ID, syncmodel.RestoreRequest{
		SpaceID: trash[0].SpaceID,
		FileID:  trash[0].FileID,
		Path:    trash[0].Path,
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(engineA.cfg.LocalRoot, "doc.txt"), "keep me")
	assertFileContent(t, filepath.Join(engineB.cfg.LocalRoot, "doc.txt"), "keep me")
}

func TestSupervisorSyncsEveryActiveSpaceUnderXCloudRoot(t *testing.T) {
	t.Parallel()

	store, err := server.NewStore(filepath.Join(t.TempDir(), "server"))
	if err != nil {
		t.Fatal(err)
	}
	account, token, err := store.CreateAccount("space-user", "", "", "password123", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSpace(account.ID, "docs", ""); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAccountSyncEnabled(account.ID, true); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(store, "", slog.Default()).Handler())
	defer srv.Close()

	rootA := filepath.Join(t.TempDir(), "client-a")
	rootB := filepath.Join(t.TempDir(), "client-b")
	supervisorA := newTestSupervisor(t, srv.URL, token, "space-a", rootA)
	supervisorB := newTestSupervisor(t, srv.URL, token, "space-b", rootB)
	ctx := context.Background()

	writeTestFile(t, filepath.Join(rootA, "docs", "guide.md"), "docs from a")
	if err := supervisorA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := supervisorB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(rootB, "docs", "guide.md"), "docs from a")
}

func newTestEngine(t *testing.T, serverURL, token, deviceID, root string) *Engine {
	t.Helper()
	engine, err := NewEngine(Config{
		StorageRoot:  root,
		ServerURL:    serverURL,
		Token:        token,
		SpaceID:      "default",
		DeviceID:     deviceID,
		Interval:     time.Second,
		Settings:     syncmodel.SyncSettings{RealtimeEnabled: false, IntervalSeconds: 1, DebounceMillis: 100},
		DeleteRemote: true,
		Log:          slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func newTestSupervisor(t *testing.T, serverURL, token, deviceID, root string) *Supervisor {
	t.Helper()
	supervisor, err := NewSupervisor(Config{
		StorageRoot: root,
		ServerURL:   serverURL,
		Token:       token,
		SpaceID:     "default",
		DeviceID:    deviceID,
		Interval:    time.Second,
		Settings:    syncmodel.SyncSettings{RealtimeEnabled: false, IntervalSeconds: 1, DebounceMillis: 100},
		Once:        true,
		Log:         slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return supervisor
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s content = %q, want %q", path, string(got), want)
	}
}

func assertFileMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s exists or stat failed with %v, want missing", path, err)
	}
}
