package client

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"xcloud/internal/fileutil"
)

type LocalConfig struct {
	ServerURL    string `json:"server_url"`
	Token        string `json:"token,omitempty"`
	SpaceID      string `json:"space_id,omitempty"`
	DeviceID     string `json:"device_id,omitempty"`
	Username     string `json:"username,omitempty"`
	DisplayName  string `json:"display_name,omitempty"`
	SyncEnabled  bool   `json:"sync_enabled"`
	DeleteRemote bool   `json:"delete_remote,omitempty"`
}

func DefaultLocalConfigPath() string {
	root := clientStateRoot()
	return filepath.Join(root, ".xcloud", "client-config.json")
}

func LoadLocalConfig(path string) (LocalConfig, error) {
	if path == "" {
		path = DefaultLocalConfigPath()
	}
	cfg := LocalConfig{}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if len(b) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func SaveLocalConfig(path string, cfg LocalConfig) error {
	if path == "" {
		path = DefaultLocalConfigPath()
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return fileutil.AtomicWrite(path, func(f *os.File) error {
		_, err := f.Write(b)
		return err
	})
}
