package server

import (
	"path/filepath"
	"testing"
)

func TestRuntimeConfigDefaults(t *testing.T) {
	cfg, err := LoadRuntimeConfig(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Domain != "ixxmi.com" {
		t.Fatalf("Domain = %q, want ixxmi.com", cfg.Domain)
	}
	if cfg.Port != 18002 {
		t.Fatalf("Port = %d, want 18002", cfg.Port)
	}
	if cfg.DataDir != "server-data" {
		t.Fatalf("DataDir = %q, want server-data", cfg.DataDir)
	}
	if cfg.PublicURL() != "http://ixxmi.com:18002" {
		t.Fatalf("PublicURL = %q", cfg.PublicURL())
	}
}

func TestRuntimeConfigSaveLoadAndListenOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	cfg := RuntimeConfig{
		Path:    path,
		Domain:  "example.com:19000",
		DataDir: "data",
	}
	cfg.Normalize()
	if cfg.Domain != "example.com" || cfg.Port != 19000 {
		t.Fatalf("normalized config = %+v", cfg)
	}
	if err := cfg.ApplyListenAddr("127.0.0.1:18003"); err != nil {
		t.Fatal(err)
	}
	if got := cfg.ListenAddr(); got != "127.0.0.1:18003" {
		t.Fatalf("ListenAddr = %q, want 127.0.0.1:18003", got)
	}
	if err := SaveRuntimeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRuntimeConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Path != path {
		t.Fatalf("Path = %q, want %q", loaded.Path, path)
	}
	if loaded.Domain != "example.com" || loaded.Port != 18003 || loaded.DataDir != "data" || loaded.ListenHost != "127.0.0.1" {
		t.Fatalf("loaded = %+v", loaded)
	}
}
