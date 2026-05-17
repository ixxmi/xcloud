package client

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPIRetriesTransientStatus(t *testing.T) {
	withFastAPIRetry(t)

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/client/status" {
			t.Fatalf("path = %s, want /v1/client/status", r.URL.Path)
		}
		if atomic.AddInt32(&calls, 1) < 3 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"account":{"id":"acct","username":"u"},"space_id":"default","sync_enabled":true}`))
	}))
	defer srv.Close()

	api := NewAPI(srv.URL, "token", "default", "device-a", "/tmp/xcloud/default")
	if _, err := api.ClientStatus(); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestAPIDoesNotRetryClientStatusError(t *testing.T) {
	withFastAPIRetry(t)

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	api := NewAPI(srv.URL, "token", "default", "device-a", "/tmp/xcloud/default")
	if _, err := api.ClientStatus(); err == nil {
		t.Fatal("ClientStatus succeeded, want error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestAPIRetriesChunkUploadAndDownload(t *testing.T) {
	withFastAPIRetry(t)

	var uploadCalls int32
	var downloadCalls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/chunks/hash-a":
			if atomic.AddInt32(&uploadCalls, 1) == 1 {
				http.Error(w, "temporary upload failure", http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/chunks/hash-a":
			if atomic.AddInt32(&downloadCalls, 1) == 1 {
				http.Error(w, "temporary download failure", http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte("chunk-data"))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	api := NewAPI(srv.URL, "token", "default", "device-a", "/tmp/xcloud/default")
	if err := api.UploadChunk("hash-a", []byte("chunk-data")); err != nil {
		t.Fatal(err)
	}
	data, err := api.DownloadChunk("hash-a")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "chunk-data" {
		t.Fatalf("downloaded data = %q, want %q", string(data), "chunk-data")
	}
	if got := atomic.LoadInt32(&uploadCalls); got != 2 {
		t.Fatalf("upload calls = %d, want 2", got)
	}
	if got := atomic.LoadInt32(&downloadCalls); got != 2 {
		t.Fatalf("download calls = %d, want 2", got)
	}
}

func TestAPIRetriesBrokenResponseBody(t *testing.T) {
	withFastAPIRetry(t)

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Content-Length", "20")
			_, _ = w.Write([]byte("partial"))
			return
		}
		_, _ = w.Write([]byte("complete"))
	}))
	defer srv.Close()

	api := NewAPI(srv.URL, "token", "default", "device-a", "/tmp/xcloud/default")
	data, err := api.DownloadChunk("hash-a")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "complete" {
		t.Fatalf("downloaded data = %q, want %q", string(data), "complete")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func withFastAPIRetry(t *testing.T) {
	t.Helper()
	oldInitial := apiRetryInitialBackoff
	oldMax := apiRetryMaxBackoff
	apiRetryInitialBackoff = time.Millisecond
	apiRetryMaxBackoff = time.Millisecond
	t.Cleanup(func() {
		apiRetryInitialBackoff = oldInitial
		apiRetryMaxBackoff = oldMax
	})
}
