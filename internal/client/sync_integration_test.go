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
	for _, folder := range store.ListFolders(account.ID) {
		if err := store.SelectFolder(account.ID, folder.ID, "default"); err != nil {
			t.Fatal(err)
		}
	}

	writeTestFile(t, filepath.Join(rootA, "note.txt"), "from a")
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(rootB, "note.txt"), "from a")

	writeTestFile(t, filepath.Join(rootB, "note.txt"), "from b")
	if err := engineB.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engineA.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, filepath.Join(rootA, "note.txt"), "from b")
}

func newTestEngine(t *testing.T, serverURL, token, deviceID, root string) *Engine {
	t.Helper()
	engine, err := NewEngine(Config{
		Root:         root,
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
