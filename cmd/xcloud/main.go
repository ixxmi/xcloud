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
	configPath := fs.String("config", env("XCLOUD_SERVER_CONFIG", server.DefaultRuntimeConfigPath), "server runtime config path")
	addr := fs.String("addr", "", "HTTP listen address; overrides config")
	data := fs.String("data", "", "server data directory; overrides config")
	token := fs.String("token", env("XCLOUD_TOKEN", ""), "Bearer token; can also use XCLOUD_TOKEN")
	if err := fs.Parse(args); err != nil {
		return err
	}
	runtimeConfig, err := server.LoadRuntimeConfig(*configPath)
	if err != nil {
		return err
	}
	if *addr != "" {
		if err := runtimeConfig.ApplyListenAddr(*addr); err != nil {
			return err
		}
	}
	if *data != "" {
		runtimeConfig.DataDir = *data
	}
	runtimeConfig.Normalize()
	store, err := server.NewStore(runtimeConfig.DataDir)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              runtimeConfig.ListenAddr(),
		Handler:           server.NewWithRuntimeConfig(store, *token, log, runtimeConfig).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Info("xcloud server listening", "addr", runtimeConfig.ListenAddr(), "public_url", runtimeConfig.PublicURL(), "data", runtimeConfig.DataDir, "config", runtimeConfig.Path)
	return srv.ListenAndServe()
}

func runClient(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	root := fs.String("root", "", "client xcloud storage root; defaults to <working-directory>/xcloud")
	serverURL := fs.String("server", "http://ixxmi.com:18002", "server URL")
	token := fs.String("token", env("XCLOUD_TOKEN", ""), "account sync token; can also use XCLOUD_TOKEN")
	clientAddr := fs.String("client-addr", "127.0.0.1:18080", "local client management console address when token is omitted")
	clientConfig := fs.String("client-config", "", "local client config path; defaults to ~/.xcloud/client-config.json")
	spaceID := fs.String("space", env("XCLOUD_SPACE", "default"), "fallback sync space ID; active spaces are loaded from the server")
	deviceID := fs.String("device", env("XCLOUD_DEVICE_ID", ""), "device ID; defaults to hostname")
	statePath := fs.String("state", "", "client supervisor state file; defaults to ~/.xcloud/discovery-state.json")
	interval := fs.Duration("interval", 10*time.Second, "sync interval")
	chunkSize := fs.Int("chunk-size", syncmodel.DefaultChunkSize, "chunk size in bytes")
	once := fs.Bool("once", false, "run one sync cycle and exit")
	deleteRemote := fs.Bool("delete-remote", true, "compatibility flag; local deletes are always propagated")
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
			Root:         storageRoot,
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
		cfg.Root = storageRoot
	}
	supervisor, err := client.NewSupervisor(cfg)
	if err != nil {
		return err
	}
	log.Info("xcloud client started", "storage_root", storageRoot, "server", *serverURL, "fallback_space", *spaceID, "interval", *interval)
	return supervisor.Run(ctx)
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
  xcloud server
  mkdir -p ./xcloud
  xcloud client
  xcloud client -root /data/xcloud -server http://ixxmi.com:18002 -token account-token

Commands:
  server    start the central sync server
  client    sync files under the local xcloud storage root by Space

`)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
