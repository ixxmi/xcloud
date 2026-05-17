package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"xcloud/internal/client"
	"xcloud/internal/server"
	"xcloud/internal/syncmodel"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	switch os.Args[1] {
	case "server":
		if err := runServer(os.Args[2:], log); err != nil {
			log.Error("server exited", "err", err)
			os.Exit(1)
		}
	case "client":
		if err := runClient(os.Args[2:], log); err != nil {
			log.Error("client exited", "err", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runServer(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	data := fs.String("data", "./xcloud-data", "server data directory")
	token := fs.String("token", env("XCLOUD_TOKEN", ""), "Bearer token; can also use XCLOUD_TOKEN")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := server.NewStore(*data)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(store, *token, log).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Info("xcloud server listening", "addr", *addr, "data", *data)
	return srv.ListenAndServe()
}

func runClient(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	root := fs.String("root", "", "directory to sync; when omitted, report discoverable local folders and sync selected ones")
	serverURL := fs.String("server", "http://127.0.0.1:8080", "server URL")
	token := fs.String("token", env("XCLOUD_TOKEN", ""), "account sync token; can also use XCLOUD_TOKEN")
	clientAddr := fs.String("client-addr", "127.0.0.1:18080", "local client management console address when token is omitted")
	clientConfig := fs.String("client-config", "", "local client config path; defaults to ~/.xcloud/client-config.json")
	spaceID := fs.String("space", env("XCLOUD_SPACE", "default"), "suggested sync space ID for gateway selection; can also use XCLOUD_SPACE")
	deviceID := fs.String("device", env("XCLOUD_DEVICE_ID", ""), "device ID; defaults to hostname")
	statePath := fs.String("state", "", "client state file for single-root mode; defaults to <root>/.xcloud/state.json")
	interval := fs.Duration("interval", 10*time.Second, "sync interval")
	chunkSize := fs.Int("chunk-size", syncmodel.DefaultChunkSize, "chunk size in bytes")
	once := fs.Bool("once", false, "run one sync cycle and exit")
	deleteRemote := fs.Bool("delete-remote", false, "propagate local deletes to server")
	if err := fs.Parse(args); err != nil {
		return err
	}
	storageRoot := defaultClientStorageRoot(*root)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := client.Config{
		Root:         *root,
		StorageRoot:  storageRoot,
		StatePath:    *statePath,
		ServerURL:    *serverURL,
		Token:        *token,
		SpaceID:      *spaceID,
		DeviceID:     *deviceID,
		Interval:     *interval,
		ChunkSize:    *chunkSize,
		Once:         *once,
		DeleteRemote: *deleteRemote,
		Log:          log,
	}
	if *token == "" {
		console, err := client.NewConsole(client.ConsoleConfig{
			Root:         *root,
			ServerURL:    *serverURL,
			ListenAddr:   *clientAddr,
			ConfigPath:   *clientConfig,
			StatePath:    *statePath,
			SpaceID:      *spaceID,
			DeviceID:     *deviceID,
			Interval:     *interval,
			ChunkSize:    *chunkSize,
			DeleteRemote: *deleteRemote,
			Log:          log,
		})
		if err != nil {
			return err
		}
		log.Info("xcloud client console started", "addr", *clientAddr, "server", *serverURL)
		return console.Run(ctx)
	}
	if *root == "" {
		supervisor, err := client.NewSupervisor(cfg)
		if err != nil {
			return err
		}
		log.Info("xcloud client started", "mode", "discover", "server", *serverURL, "suggested_space", *spaceID, "interval", *interval)
		return supervisor.Run(ctx)
	}
	engine, err := client.NewEngine(cfg)
	if err != nil {
		return err
	}
	log.Info("xcloud client started", "mode", "single-root", "root", *root, "server", *serverURL, "suggested_space", *spaceID, "interval", *interval)
	return engine.Run(ctx)
}

func defaultClientStorageRoot(root string) string {
	if root != "" {
		if abs, err := filepath.Abs(root); err == nil {
			return abs
		}
		return root
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "xcloud"
	}
	return filepath.Join(cwd, "xcloud")
}

func usage() {
	fmt.Fprintf(os.Stderr, `xcloud - secure central file sync MVP

Usage:
  xcloud server -addr :8080 -data ./xcloud-data
  mkdir -p ./xcloud
  xcloud client -server http://127.0.0.1:8080
  mkdir -p ./docs
  xcloud client -root ./docs -server http://127.0.0.1:8080 -token account-token -space default

Commands:
  server    start the central sync server
  client    report local folders and sync selected folders after gateway selection

`)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
