package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const DefaultRuntimeConfigPath = "xcloud-server.json"

type RuntimeConfig struct {
	Path       string `json:"-"`
	Domain     string `json:"domain"`
	Port       int    `json:"port"`
	DataDir    string `json:"data_dir"`
	ListenHost string `json:"listen_host,omitempty"`
}

func DefaultRuntimeConfig(path string) RuntimeConfig {
	if strings.TrimSpace(path) == "" {
		path = DefaultRuntimeConfigPath
	}
	return RuntimeConfig{
		Path:    path,
		Domain:  "ixxmi.com",
		Port:    18002,
		DataDir: "server-data",
	}
}

func LoadRuntimeConfig(path string) (RuntimeConfig, error) {
	cfg := DefaultRuntimeConfig(path)
	cfgPath := cfg.Path
	data, err := os.ReadFile(cfg.Path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if len(data) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	cfg.Path = cfgPath
	cfg.Normalize()
	return cfg, nil
}

func SaveRuntimeConfig(cfg RuntimeConfig) error {
	cfg.Normalize()
	if strings.TrimSpace(cfg.Path) == "" {
		cfg.Path = DefaultRuntimeConfigPath
	}
	dir := filepath.Dir(cfg.Path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfg.Path, append(data, '\n'), 0o644)
}

func (cfg *RuntimeConfig) Normalize() {
	cfg.Domain = strings.TrimSpace(cfg.Domain)
	if cfg.Domain == "" {
		cfg.Domain = "ixxmi.com"
	}
	cfg.Domain = strings.TrimPrefix(strings.TrimPrefix(cfg.Domain, "https://"), "http://")
	cfg.Domain = strings.TrimSuffix(cfg.Domain, "/")
	if host, port, err := net.SplitHostPort(cfg.Domain); err == nil {
		cfg.Domain = host
		if parsed, parseErr := strconv.Atoi(port); parseErr == nil && parsed > 0 {
			cfg.Port = parsed
		}
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		cfg.Port = 18002
	}
	cfg.DataDir = strings.TrimSpace(cfg.DataDir)
	if cfg.DataDir == "" {
		cfg.DataDir = "server-data"
	}
	cfg.ListenHost = strings.TrimSpace(cfg.ListenHost)
}

func (cfg RuntimeConfig) ListenAddr() string {
	cfg.Normalize()
	return net.JoinHostPort(cfg.ListenHost, strconv.Itoa(cfg.Port))
}

func (cfg RuntimeConfig) PublicURL() string {
	cfg.Normalize()
	return fmt.Sprintf("http://%s:%d", cfg.Domain, cfg.Port)
}

func (cfg *RuntimeConfig) ApplyListenAddr(addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.HasPrefix(addr, ":") {
			host = ""
			portText = strings.TrimPrefix(addr, ":")
		} else {
			return err
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("invalid listen port %q", portText)
	}
	cfg.ListenHost = host
	cfg.Port = port
	return nil
}
