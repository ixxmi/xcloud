package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"xcloud/internal/fileutil"
	"xcloud/internal/syncmodel"
)

type Config struct {
	Root         string
	StatePath    string
	ServerURL    string
	Token        string
	DeviceID     string
	Interval     time.Duration
	ChunkSize    int
	Once         bool
	DeleteRemote bool
	Log          *slog.Logger
}

type Engine struct {
	cfg   Config
	api   *API
	state *State
	log   *slog.Logger
}

type localScanEntry struct {
	Path        string
	AbsPath     string
	Size        int64
	ModTimeUnix int64
	Hash        string
	Chunks      []syncmodel.ChunkRef
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.Root == "" {
		return nil, errors.New("root is required")
	}
	if cfg.ServerURL == "" {
		return nil, errors.New("server URL is required")
	}
	if cfg.DeviceID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "device"
		}
		cfg.DeviceID = host
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = syncmodel.DefaultChunkSize
	}
	if cfg.StatePath == "" {
		cfg.StatePath = filepath.Join(cfg.Root, ".xcloud", "state.json")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	rootAbs, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	cfg.Root = rootAbs
	state, err := OpenState(cfg.StatePath)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:   cfg,
		api:   NewAPI(cfg.ServerURL, cfg.Token),
		state: state,
		log:   cfg.Log,
	}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	if e.cfg.Once {
		return e.SyncOnce(ctx)
	}
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := e.SyncOnce(ctx); err != nil {
			e.log.Error("sync failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) SyncOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.pullRemote(ctx); err != nil {
		return err
	}
	if err := e.pushLocal(ctx); err != nil {
		return err
	}
	return e.pullRemote(ctx)
}

func (e *Engine) pushLocal(ctx context.Context) error {
	scan, err := e.scanLocal()
	if err != nil {
		return err
	}
	for rel, entry := range scan {
		if err := ctx.Err(); err != nil {
			return err
		}
		known, ok := e.state.Get(rel)
		if ok && known.State == syncmodel.EntryFile && known.Hash == entry.Hash && known.Size == entry.Size {
			if known.ModTimeUnix != entry.ModTimeUnix {
				known.ModTimeUnix = entry.ModTimeUnix
				known.UpdatedAt = time.Now().Unix()
				if err := e.state.Set(rel, known); err != nil {
					return err
				}
			}
			continue
		}
		if err := e.uploadFile(ctx, entry, known); err != nil {
			return err
		}
	}

	if !e.cfg.DeleteRemote {
		return nil
	}
	for rel, known := range e.state.Snapshot() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if known.State != syncmodel.EntryFile {
			continue
		}
		if _, ok := scan[rel]; ok {
			continue
		}
		e.log.Info("delete remote tombstone", "path", rel)
		resp, err := e.api.Delete(syncmodel.DeleteRequest{
			OperationID: fileutil.NewID(),
			DeviceID:    e.cfg.DeviceID,
			Path:        rel,
			FileID:      known.FileID,
			BaseVersion: known.VersionID,
		})
		if err != nil {
			return err
		}
		if resp.Status == "ok" {
			_ = e.state.Set(rel, syncmodel.LocalFileState{
				Path:      rel,
				FileID:    known.FileID,
				VersionID: resp.Version.VersionID,
				State:     syncmodel.EntryDeleted,
				UpdatedAt: time.Now().Unix(),
			})
		}
	}
	return nil
}

func (e *Engine) uploadFile(ctx context.Context, entry localScanEntry, known syncmodel.LocalFileState) error {
	hashes := make([]string, 0, len(entry.Chunks))
	for _, chunk := range entry.Chunks {
		hashes = append(hashes, chunk.Hash)
	}
	missing, err := e.api.CheckChunks(hashes)
	if err != nil {
		return err
	}
	missingSet := map[string]bool{}
	for _, h := range missing {
		missingSet[h] = true
	}
	if len(missingSet) > 0 {
		err = fileutil.WriteFileChunks(entry.AbsPath, e.cfg.ChunkSize, func(ref syncmodel.ChunkRef, data []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !missingSet[ref.Hash] {
				return nil
			}
			e.log.Info("upload chunk", "path", entry.Path, "chunk", ref.Index, "size", ref.Size)
			return e.api.UploadChunk(ref.Hash, data)
		})
		if err != nil {
			return err
		}
	}

	req := syncmodel.CommitRequest{
		OperationID: fileutil.NewID(),
		DeviceID:    e.cfg.DeviceID,
		Manifest: syncmodel.Manifest{
			FileID:      known.FileID,
			Path:        entry.Path,
			BaseVersion: known.VersionID,
			Size:        entry.Size,
			Hash:        entry.Hash,
			Chunks:      entry.Chunks,
			ModTimeUnix: entry.ModTimeUnix,
		},
	}
	e.log.Info("commit file", "path", entry.Path, "size", entry.Size)
	resp, err := e.api.Commit(req)
	if err != nil {
		return err
	}
	if resp.Conflict {
		e.log.Warn("server created conflict copy", "path", entry.Path, "conflict_path", resp.ConflictPath)
	}
	localPath := resp.Version.Path
	if localPath != entry.Path && resp.ConflictPath != "" {
		target := filepath.Join(e.cfg.Root, filepath.FromSlash(resp.ConflictPath))
		if err := fileutil.EnsureParent(target); err != nil {
			return err
		}
		if err := os.Rename(entry.AbsPath, target); err != nil {
			return err
		}
		_ = e.state.Delete(entry.Path)
	}
	return e.state.Set(localPath, stateFromVersion(resp.Version))
}

func (e *Engine) pullRemote(ctx context.Context) error {
	events, err := e.api.Events(e.state.LastEventSeq())
	if err != nil {
		return err
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].Seq < events[j].Seq
	})
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if event.DeviceID == e.cfg.DeviceID {
			if err := e.state.SetLastEventSeq(event.Seq); err != nil {
				return err
			}
			continue
		}
		if err := e.applyVersion(ctx, event.Version); err != nil {
			return err
		}
		if err := e.state.SetLastEventSeq(event.Seq); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) applyVersion(ctx context.Context, version syncmodel.FileVersion) error {
	rel, err := fileutil.SafeRel(e.cfg.Root, version.Path)
	if err != nil {
		return err
	}
	target := filepath.Join(e.cfg.Root, filepath.FromSlash(rel))
	known, hasKnown := e.state.Get(rel)
	switch version.State {
	case syncmodel.EntryDeleted:
		if hasKnown && known.VersionID == version.VersionID {
			return nil
		}
		if hasLocalDivergence(target, known) {
			conflict := conflictLocalPath(target, e.cfg.DeviceID)
			e.log.Warn("local delete conflict preserved", "path", rel, "conflict", conflict)
			if err := fileutil.EnsureParent(conflict); err != nil {
				return err
			}
			if err := os.Rename(target, conflict); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return e.state.Set(rel, stateFromVersion(version))
	case syncmodel.EntryFile:
		if hasKnown && known.VersionID == version.VersionID {
			return nil
		}
		if hasLocalDivergence(target, known) {
			conflict := conflictLocalPath(target, e.cfg.DeviceID)
			e.log.Warn("local edit conflict preserved", "path", rel, "conflict", conflict)
			if err := fileutil.EnsureParent(conflict); err != nil {
				return err
			}
			if err := os.Rename(target, conflict); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := e.downloadVersion(ctx, target, version); err != nil {
			return err
		}
		return e.state.Set(rel, stateFromVersion(version))
	default:
		return fmt.Errorf("unknown version state %q", version.State)
	}
}

func (e *Engine) downloadVersion(ctx context.Context, target string, version syncmodel.FileVersion) error {
	e.log.Info("download file", "path", version.Path, "size", version.Size)
	err := fileutil.AtomicWrite(target, func(f *os.File) error {
		for _, chunk := range version.Chunks {
			if err := ctx.Err(); err != nil {
				return err
			}
			data, err := e.api.DownloadChunk(chunk.Hash)
			if err != nil {
				return err
			}
			if int64(len(data)) != chunk.Size {
				return fmt.Errorf("chunk %s size mismatch", chunk.Hash)
			}
			if fileutil.HashBytes(data) != chunk.Hash {
				return fmt.Errorf("chunk %s hash mismatch", chunk.Hash)
			}
			if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	hash, _, size, err := fileutil.HashFile(target, e.cfg.ChunkSize)
	if err != nil {
		return err
	}
	if hash != version.Hash || size != version.Size {
		return fmt.Errorf("downloaded file verification failed for %s", version.Path)
	}
	if version.ModTimeUnix > 0 {
		t := time.Unix(version.ModTimeUnix, 0)
		_ = os.Chtimes(target, t, t)
	}
	return nil
}

func (e *Engine) scanLocal() (map[string]localScanEntry, error) {
	root := e.cfg.Root
	out := map[string]localScanEntry{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == ".xcloud" || strings.HasPrefix(rel, ".xcloud/") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		hash, chunks, size, err := fileutil.HashFile(path, e.cfg.ChunkSize)
		if err != nil {
			return err
		}
		out[rel] = localScanEntry{
			Path:        rel,
			AbsPath:     path,
			Size:        size,
			ModTimeUnix: info.ModTime().Unix(),
			Hash:        hash,
			Chunks:      chunks,
		}
		return nil
	})
	return out, err
}

func hasLocalDivergence(path string, known syncmodel.LocalFileState) bool {
	if known.State != syncmodel.EntryFile {
		_, err := os.Stat(path)
		return err == nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil || !info.Mode().IsRegular() {
		return true
	}
	if info.Size() != known.Size {
		return true
	}
	if known.ModTimeUnix != 0 && info.ModTime().Unix() == known.ModTimeUnix {
		return false
	}
	hash, _, size, err := fileutil.HashFile(path, syncmodel.DefaultChunkSize)
	if err != nil {
		return true
	}
	return size != known.Size || hash != known.Hash
}

func conflictLocalPath(path, deviceID string) string {
	ext := filepath.Ext(path)
	stem := strings.TrimSuffix(path, ext)
	safeDevice := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, deviceID)
	return fmt.Sprintf("%s (local conflict from %s at %s)%s", stem, safeDevice, time.Now().Format("20060102-150405"), ext)
}

func stateFromVersion(version syncmodel.FileVersion) syncmodel.LocalFileState {
	return syncmodel.LocalFileState{
		Path:        version.Path,
		FileID:      version.FileID,
		VersionID:   version.VersionID,
		State:       version.State,
		Size:        version.Size,
		Hash:        version.Hash,
		Chunks:      append([]syncmodel.ChunkRef(nil), version.Chunks...),
		ModTimeUnix: version.ModTimeUnix,
		UpdatedAt:   time.Now().Unix(),
	}
}
