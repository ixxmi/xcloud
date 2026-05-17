package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"xcloud/internal/fileutil"
	"xcloud/internal/syncmodel"
)

type Config struct {
	Root         string
	StorageRoot  string
	LocalRoot    string
	StatePath    string
	ServerURL    string
	Token        string
	SpaceID      string
	DeviceID     string
	Interval     time.Duration
	Settings     syncmodel.SyncSettings
	ChunkSize    int
	Once         bool
	DeleteRemote bool
	Log          *slog.Logger
}

type Engine struct {
	cfg     Config
	api     *API
	state   *State
	log     *slog.Logger
	mu      sync.Mutex
	running bool
}

type Supervisor struct {
	cfg           Config
	api           *API
	state         *State
	engines       map[string]*Engine
	engineStarted map[string]bool
	log           *slog.Logger
	mu            sync.Mutex
}

type localScanEntry struct {
	Path        string
	AbsPath     string
	Size        int64
	ModTimeUnix int64
	Hash        string
	Chunks      []syncmodel.ChunkRef
}

var errWatchRootChanged = errors.New("watch root changed")

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.ServerURL == "" {
		return nil, errors.New("server URL is required")
	}
	if cfg.Root == "" {
		return nil, errors.New("root is required for single-folder sync")
	}
	if cfg.SpaceID == "" {
		cfg.SpaceID = "default"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	cfg.Settings = normalizeClientSettings(cfg.Settings, cfg.Interval)
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = syncmodel.DefaultChunkSize
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	rootAbs, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	cfg.Root = rootAbs
	if cfg.LocalRoot == "" && cfg.StorageRoot == "" {
		cfg.LocalRoot = cfg.Root
		cfg.StorageRoot = cfg.Root
	} else if cfg.StorageRoot == "" {
		cfg.StorageRoot = cfg.LocalRoot
	}
	storageRootAbs, err := filepath.Abs(cfg.StorageRoot)
	if err != nil {
		return nil, err
	}
	cfg.StorageRoot = storageRootAbs
	if cfg.LocalRoot == "" {
		cfg.LocalRoot = localRootFor(cfg.StorageRoot, cfg.Root, cfg.SpaceID)
	}
	cfg.DeviceID = defaultDeviceID(cfg.DeviceID, cfg.Root, cfg.StorageRoot, cfg.ServerURL)
	localRootAbs, err := filepath.Abs(cfg.LocalRoot)
	if err != nil {
		return nil, err
	}
	cfg.LocalRoot = localRootAbs
	if err := os.MkdirAll(cfg.LocalRoot, 0o755); err != nil {
		return nil, err
	}
	if cfg.StatePath == "" {
		cfg.StatePath = filepath.Join(cfg.LocalRoot, ".xcloud", "state.json")
	}
	state, err := OpenState(cfg.StatePath)
	if err != nil {
		return nil, err
	}
	if err := state.SeedSpace(cfg.SpaceID); err != nil {
		return nil, err
	}
	return &Engine{
		cfg:   cfg,
		api:   NewAPI(cfg.ServerURL, cfg.Token, cfg.SpaceID, cfg.DeviceID, cfg.Root),
		state: state,
		log:   cfg.Log,
	}, nil
}

func NewSupervisor(cfg Config) (*Supervisor, error) {
	if cfg.ServerURL == "" {
		return nil, errors.New("server URL is required")
	}
	if cfg.SpaceID == "" {
		cfg.SpaceID = "default"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	cfg.Settings = normalizeClientSettings(cfg.Settings, cfg.Interval)
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = syncmodel.DefaultChunkSize
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		cfg.Root = filepath.Join(cwd, "xcloud")
	}
	rootAbs, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	cfg.Root = rootAbs
	if cfg.StorageRoot == "" {
		cfg.StorageRoot = cfg.Root
	}
	storageRootAbs, err := filepath.Abs(cfg.StorageRoot)
	if err != nil {
		return nil, err
	}
	cfg.StorageRoot = storageRootAbs
	if err := os.MkdirAll(cfg.StorageRoot, 0o755); err != nil {
		return nil, err
	}
	cfg.DeviceID = defaultDeviceID(cfg.DeviceID, cfg.Root, cfg.StorageRoot, cfg.ServerURL)
	if cfg.StatePath == "" {
		cfg.StatePath = filepath.Join(clientStateRoot(), ".xcloud", "discovery-state.json")
	}
	state, err := OpenState(cfg.StatePath)
	if err != nil {
		return nil, err
	}
	return &Supervisor{
		cfg:           cfg,
		api:           NewAPI(cfg.ServerURL, cfg.Token, cfg.SpaceID, cfg.DeviceID, ""),
		state:         state,
		engines:       map[string]*Engine{},
		engineStarted: map[string]bool{},
		log:           cfg.Log,
	}, nil
}

func normalizeClientSettings(settings syncmodel.SyncSettings, fallbackInterval time.Duration) syncmodel.SyncSettings {
	if settings.IntervalSeconds <= 0 && fallbackInterval > 0 {
		settings.IntervalSeconds = int(fallbackInterval.Seconds())
	}
	if settings.IntervalSeconds <= 0 {
		settings.IntervalSeconds = syncmodel.DefaultSyncIntervalSeconds
	}
	if settings.DebounceMillis <= 0 {
		settings.DebounceMillis = syncmodel.DefaultSyncDebounceMillis
	}
	return syncmodel.NormalizeSyncSettings(settings)
}

func applySettingsToConfig(cfg *Config, settings syncmodel.SyncSettings) {
	cfg.Settings = normalizeClientSettings(settings, cfg.Interval)
	cfg.Interval = time.Duration(cfg.Settings.IntervalSeconds) * time.Second
}

func (s *Supervisor) Run(ctx context.Context) error {
	if s.cfg.Once {
		return s.SyncOnce(ctx)
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := s.SyncOnce(ctx); err != nil {
			s.log.Error("sync failed", "err", err)
		}
		ticker.Reset(s.cfg.Interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) SyncOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	enabled, err := s.accountSyncEnabled()
	if err != nil {
		s.reportRecord(syncmodel.SyncActionScan, syncmodel.SyncRecordStatusFailed, "", err, 0)
		return err
	}
	if !enabled {
		s.log.Info("account sync disabled; waiting for enable")
		return nil
	}
	candidates := []string{s.cfg.Root}
	host, _ := os.Hostname()
	status, err := s.api.FolderStatus()
	if err != nil {
		s.reportRecord(syncmodel.SyncActionScan, syncmodel.SyncRecordStatusFailed, "", err, 0)
		return err
	}
	applySettingsToConfig(&s.cfg, status.Settings)
	if err := s.applyStorageRoot(status.StorageRoot); err != nil {
		return err
	}
	candidates[0] = s.cfg.Root
	for _, req := range status.Requests {
		if err := s.reportChildren(host, req); err != nil {
			return err
		}
	}
	for _, folder := range status.Selected {
		if !containsString(candidates, folder.RootPath) {
			candidates = append(candidates, folder.RootPath)
		}
	}
	sort.Strings(candidates)
	selected := 0
	for _, root := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		reportKey := s.reportedDirKey(root)
		if s.state.DirReported(reportKey) {
			continue
		}
		resp, err := s.api.ReportFolder(syncmodel.FolderReportRequest{
			DeviceID:         s.cfg.DeviceID,
			Hostname:         host,
			RootPath:         root,
			StorageRoot:      s.cfg.StorageRoot,
			Depth:            0,
			SuggestedSpaceID: s.cfg.SpaceID,
		})
		if err != nil {
			return err
		}
		if err := s.state.MarkDirReported(reportKey); err != nil {
			return err
		}
		if !resp.Selected || resp.Space == nil {
			s.log.Info("client folder reported; waiting for gateway selection",
				"device", s.cfg.DeviceID,
				"root", root,
				"suggested_space", s.cfg.SpaceID,
				"status", resp.Folder.Status,
			)
			continue
		}
	}
	status, err = s.api.FolderStatus()
	if err != nil {
		return err
	}
	for _, folder := range status.Selected {
		selected++
		engine, err := s.engineFor(folder.RootPath, folder.SpaceID)
		if err != nil {
			return err
		}
		if s.cfg.Once {
			if err := engine.syncOnce(ctx); err != nil {
				return err
			}
			continue
		}
		s.startEngine(ctx, folder.RootPath, folder.SpaceID, engine)
	}
	if len(candidates) == 0 {
		s.log.Info("no local folders discovered for reporting")
	} else if selected == 0 {
		s.log.Info("no selected folders yet; waiting for gateway selection", "candidates", len(candidates))
	}
	return nil
}

func (s *Supervisor) accountSyncEnabled() (bool, error) {
	status, err := s.api.ClientStatus()
	if err != nil {
		return false, err
	}
	applySettingsToConfig(&s.cfg, status.Settings)
	if err := s.applyStorageRoot(status.StorageRoot); err != nil {
		return false, err
	}
	return status.SyncEnabled, nil
}

func (s *Supervisor) applyStorageRoot(storageRoot string) error {
	storageRoot = strings.TrimSpace(storageRoot)
	if storageRoot == "" {
		return nil
	}
	rootAbs, err := filepath.Abs(storageRoot)
	if err != nil {
		return err
	}
	if rootAbs == s.cfg.StorageRoot {
		return nil
	}
	if err := os.MkdirAll(rootAbs, 0o755); err != nil {
		return err
	}
	s.cfg.StorageRoot = rootAbs
	return nil
}

func (s *Supervisor) reportChildren(host string, req syncmodel.FolderDiscoveryRequest) error {
	children, err := discoverChildFolders(req.RootPath)
	if err != nil {
		s.log.Warn("cannot discover child folders", "root", req.RootPath, "err", err)
		return s.api.ChildrenComplete(syncmodel.FolderChildrenCompleteRequest{
			DeviceID: s.cfg.DeviceID,
			RootPath: req.RootPath,
		})
	}
	for _, child := range children {
		reportKey := s.reportedDirKey(child)
		if s.state.DirReported(reportKey) {
			continue
		}
		if _, err := s.api.ReportFolder(syncmodel.FolderReportRequest{
			DeviceID:         s.cfg.DeviceID,
			Hostname:         host,
			RootPath:         child,
			StorageRoot:      s.cfg.StorageRoot,
			ParentPath:       req.RootPath,
			Depth:            req.Depth,
			SuggestedSpaceID: s.cfg.SpaceID,
		}); err != nil {
			return err
		}
		if err := s.state.MarkDirReported(reportKey); err != nil {
			return err
		}
		s.log.Info("reported child folder", "parent", req.RootPath, "root", child)
	}
	return s.api.ChildrenComplete(syncmodel.FolderChildrenCompleteRequest{
		DeviceID: s.cfg.DeviceID,
		RootPath: req.RootPath,
	})
}

func (s *Supervisor) reportedDirKey(path string) string {
	sum := sha256.Sum256([]byte(s.cfg.Token))
	tokenPrefix := hex.EncodeToString(sum[:])[:12]
	return s.cfg.ServerURL + "\x00" + s.cfg.DeviceID + "\x00" + tokenPrefix + "\x00" + path
}

func (s *Supervisor) engineFor(root, spaceID string) (*Engine, error) {
	key := root + "\x00" + spaceID
	localRoot := localRootFor(s.cfg.StorageRoot, root, spaceID)
	s.mu.Lock()
	if engine := s.engines[key]; engine != nil {
		if engine.cfg.LocalRoot != localRoot {
			engine.cfg.StorageRoot = s.cfg.StorageRoot
			if err := engine.switchRoot(localRoot); err != nil {
				s.mu.Unlock()
				return nil, err
			}
		}
		s.mu.Unlock()
		return engine, nil
	}
	s.mu.Unlock()
	cfg := s.cfg
	cfg.Root = root
	cfg.LocalRoot = localRoot
	cfg.SpaceID = spaceID
	cfg.StatePath = filepath.Join(cfg.LocalRoot, ".xcloud", "state-"+safeStateName(spaceID)+".json")
	engine, err := NewEngine(cfg)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.engines[key] = engine
	s.mu.Unlock()
	return engine, nil
}

func (s *Supervisor) startEngine(ctx context.Context, root, spaceID string, engine *Engine) {
	key := root + "\x00" + spaceID
	s.mu.Lock()
	if s.engineStarted[key] {
		s.mu.Unlock()
		return
	}
	s.engineStarted[key] = true
	s.mu.Unlock()
	go func() {
		err := engine.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("selected folder sync stopped", "root", root, "space", spaceID, "err", err)
		}
		s.mu.Lock()
		delete(s.engineStarted, key)
		s.mu.Unlock()
	}()
}

func (s *Supervisor) reportRecord(action, status, path string, err error, duration time.Duration) {
	req := syncmodel.SyncRecordRequest{
		SpaceID:        s.cfg.SpaceID,
		DeviceID:       s.cfg.DeviceID,
		Path:           path,
		Action:         action,
		Status:         status,
		DurationMillis: duration.Milliseconds(),
	}
	if err != nil {
		req.Error = err.Error()
	}
	_ = s.api.ReportSyncRecord(req)
}

func (e *Engine) Run(ctx context.Context) error {
	if e.cfg.Once {
		return e.SyncOnce(ctx)
	}
	if e.cfg.Settings.RealtimeEnabled {
		return e.runRealtime(ctx)
	}
	return e.runInterval(ctx)
}

func (e *Engine) runInterval(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := e.SyncOnce(ctx); err != nil {
			e.log.Error("sync failed", "err", err)
		}
		ticker.Reset(e.cfg.Interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) runRealtime(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := e.runRealtimeWatch(ctx)
		if errors.Is(err, errWatchRootChanged) {
			continue
		}
		return err
	}
}

func (e *Engine) runRealtimeWatch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		e.reportRecord(syncmodel.SyncActionWatch, syncmodel.SyncRecordStatusFailed, "", err, 0)
		return e.runInterval(ctx)
	}
	defer watcher.Close()
	watchedRoot := e.cfg.LocalRoot
	if err := e.watchTree(watcher); err != nil {
		e.reportRecord(syncmodel.SyncActionWatch, syncmodel.SyncRecordStatusFailed, "", err, 0)
		e.log.Warn("filesystem watch failed; falling back to interval sync", "root", e.cfg.LocalRoot, "err", err)
		return e.runInterval(ctx)
	}
	trigger := make(chan struct{}, 1)
	triggerSync := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	triggerSync()
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.Events:
			if !ok {
				return ctx.Err()
			}
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) != 0 {
				if info, statErr := os.Stat(event.Name); statErr == nil && info.IsDir() {
					_ = filepath.WalkDir(event.Name, func(path string, d os.DirEntry, walkErr error) error {
						if walkErr != nil || !d.IsDir() || shouldSkipDiscoveredDir(d.Name()) {
							return nil
						}
						_ = watcher.Add(path)
						return nil
					})
				}
				triggerSync()
			}
		case err, ok := <-watcher.Errors:
			if ok && err != nil {
				e.reportRecord(syncmodel.SyncActionWatch, syncmodel.SyncRecordStatusFailed, "", err, 0)
				e.log.Warn("filesystem watch error", "root", e.cfg.LocalRoot, "err", err)
			}
		case <-ticker.C:
			triggerSync()
			ticker.Reset(e.cfg.Interval)
		case <-trigger:
			debounce := time.Duration(e.cfg.Settings.DebounceMillis) * time.Millisecond
			timer := time.NewTimer(debounce)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			if err := e.SyncOnce(ctx); err != nil {
				e.log.Error("sync failed", "err", err)
			}
			if e.cfg.LocalRoot != watchedRoot {
				return errWatchRootChanged
			}
		}
	}
}

func (e *Engine) watchTree(watcher *fsnotify.Watcher) error {
	return filepath.WalkDir(e.cfg.LocalRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(e.cfg.LocalRoot, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if rel == ".xcloud" || strings.HasPrefix(rel, ".xcloud/") || shouldSkipDiscoveredDir(d.Name()) {
			if path != e.cfg.LocalRoot {
				return filepath.SkipDir
			}
		}
		return watcher.Add(path)
	})
}

func (e *Engine) SyncOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return nil
	}
	e.running = true
	defer func() {
		e.running = false
		e.mu.Unlock()
	}()
	enabled, err := e.accountSyncEnabled()
	if err != nil {
		return err
	}
	if !enabled {
		e.log.Info("account sync disabled; waiting for enable")
		return nil
	}
	return e.syncOnce(ctx)
}

func (e *Engine) accountSyncEnabled() (bool, error) {
	status, err := e.api.ClientStatus()
	if err != nil {
		return false, err
	}
	applySettingsToConfig(&e.cfg, status.Settings)
	if strings.TrimSpace(status.StorageRoot) != "" {
		storageRootAbs, err := filepath.Abs(status.StorageRoot)
		if err != nil {
			return false, err
		}
		if storageRootAbs != e.cfg.StorageRoot {
			e.cfg.StorageRoot = storageRootAbs
		}
		targetRoot := localRootFor(e.cfg.StorageRoot, e.cfg.Root, e.cfg.SpaceID)
		if targetRoot != e.cfg.LocalRoot {
			if err := e.switchRoot(targetRoot); err != nil {
				return false, err
			}
		}
	}
	return status.SyncEnabled, nil
}

func (e *Engine) switchRoot(root string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(rootAbs, 0o755); err != nil {
		return err
	}
	e.cfg.LocalRoot = rootAbs
	e.cfg.StatePath = filepath.Join(rootAbs, ".xcloud", "state-"+safeStateName(e.cfg.SpaceID)+".json")
	state, err := OpenState(e.cfg.StatePath)
	if err != nil {
		return err
	}
	if err := state.SeedSpace(e.cfg.SpaceID); err != nil {
		return err
	}
	e.state = state
	e.api.SetSyncContext(e.cfg.SpaceID, e.cfg.DeviceID, e.cfg.Root)
	return nil
}

func (e *Engine) syncOnce(ctx context.Context) error {
	selected, err := e.ensureFolderSelected()
	if err != nil {
		return err
	}
	if !selected {
		return nil
	}
	if err := e.pullRemote(ctx); err != nil {
		return err
	}
	if err := e.pushLocal(ctx); err != nil {
		return err
	}
	return e.pullRemote(ctx)
}

func (e *Engine) ensureFolderSelected() (bool, error) {
	host, _ := os.Hostname()
	resp, err := e.api.ReportFolder(syncmodel.FolderReportRequest{
		DeviceID:         e.cfg.DeviceID,
		Hostname:         host,
		RootPath:         e.cfg.Root,
		StorageRoot:      e.cfg.StorageRoot,
		SuggestedSpaceID: e.cfg.SpaceID,
	})
	if err != nil {
		return false, err
	}
	if !resp.Selected || resp.Space == nil {
		e.log.Info("client folder reported; waiting for gateway selection",
			"device", e.cfg.DeviceID,
			"root", e.cfg.Root,
			"suggested_space", e.cfg.SpaceID,
			"status", resp.Folder.Status,
		)
		return false, nil
	}
	if resp.Space.ID != e.cfg.SpaceID {
		e.log.Info("gateway assigned sync space", "device", e.cfg.DeviceID, "root", e.cfg.Root, "space", resp.Space.ID)
	}
	e.cfg.SpaceID = resp.Space.ID
	e.api.SetSyncContext(resp.Space.ID, e.cfg.DeviceID, e.cfg.Root)
	if err := e.state.EnsureSpace(resp.Space.ID); err != nil {
		return false, err
	}
	return true, nil
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
		started := time.Now()
		resp, err := e.api.Delete(syncmodel.DeleteRequest{
			OperationID: fileutil.NewID(),
			DeviceID:    e.cfg.DeviceID,
			RootPath:    e.cfg.Root,
			Path:        rel,
			FileID:      known.FileID,
			BaseVersion: known.VersionID,
		})
		if err != nil {
			e.reportRecord(syncmodel.SyncActionDelete, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
			return err
		}
		if resp.Status == "ok" {
			if err := e.state.Set(rel, syncmodel.LocalFileState{
				Path:      rel,
				FileID:    known.FileID,
				VersionID: resp.Version.VersionID,
				State:     syncmodel.EntryDeleted,
				UpdatedAt: time.Now().Unix(),
			}); err != nil {
				e.reportRecord(syncmodel.SyncActionDelete, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
				return err
			}
			e.reportRecord(syncmodel.SyncActionDelete, syncmodel.SyncRecordStatusSuccess, rel, nil, time.Since(started))
		}
	}
	return nil
}

func (e *Engine) reportRecord(action, status, path string, err error, duration time.Duration) {
	req := syncmodel.SyncRecordRequest{
		SpaceID:        e.cfg.SpaceID,
		DeviceID:       e.cfg.DeviceID,
		RootPath:       e.cfg.Root,
		Path:           path,
		Action:         action,
		Status:         status,
		DurationMillis: duration.Milliseconds(),
	}
	if err != nil {
		req.Error = err.Error()
	}
	_ = e.api.ReportSyncRecord(req)
}

func (e *Engine) uploadFile(ctx context.Context, entry localScanEntry, known syncmodel.LocalFileState) error {
	started := time.Now()
	hashes := make([]string, 0, len(entry.Chunks))
	for _, chunk := range entry.Chunks {
		hashes = append(hashes, chunk.Hash)
	}
	missing, err := e.api.CheckChunks(hashes)
	if err != nil {
		e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusFailed, entry.Path, err, time.Since(started))
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
			e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusFailed, entry.Path, err, time.Since(started))
			return err
		}
	}

	req := syncmodel.CommitRequest{
		OperationID: fileutil.NewID(),
		DeviceID:    e.cfg.DeviceID,
		RootPath:    e.cfg.Root,
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
		e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusFailed, entry.Path, err, time.Since(started))
		return err
	}
	if resp.Conflict {
		e.log.Warn("server created conflict copy", "path", entry.Path, "conflict_path", resp.ConflictPath)
		e.reportRecord(syncmodel.SyncActionConflict, syncmodel.SyncRecordStatusSuccess, entry.Path, nil, time.Since(started))
	}
	localPath := resp.Version.Path
	if localPath != entry.Path && resp.ConflictPath != "" {
		target := filepath.Join(e.cfg.LocalRoot, filepath.FromSlash(resp.ConflictPath))
		if err := fileutil.EnsureParent(target); err != nil {
			e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusFailed, entry.Path, err, time.Since(started))
			return err
		}
		if err := os.Rename(entry.AbsPath, target); err != nil {
			e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusFailed, entry.Path, err, time.Since(started))
			return err
		}
		_ = e.state.Delete(entry.Path)
	}
	if err := e.state.Set(localPath, stateFromVersion(resp.Version)); err != nil {
		e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusFailed, entry.Path, err, time.Since(started))
		return err
	}
	e.reportRecord(syncmodel.SyncActionUpload, syncmodel.SyncRecordStatusSuccess, localPath, nil, time.Since(started))
	return nil
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
		if event.DeviceID == e.cfg.DeviceID && event.RootPath == e.cfg.Root {
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
	started := time.Now()
	rel, err := fileutil.CleanRel(version.Path)
	if err != nil {
		e.reportRecord(syncmodel.SyncActionDownload, syncmodel.SyncRecordStatusFailed, version.Path, err, time.Since(started))
		return err
	}
	target := filepath.Join(e.cfg.LocalRoot, filepath.FromSlash(rel))
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
				e.reportRecord(syncmodel.SyncActionConflict, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
				return err
			}
			e.reportRecord(syncmodel.SyncActionConflict, syncmodel.SyncRecordStatusSuccess, rel, nil, time.Since(started))
		} else if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			e.reportRecord(syncmodel.SyncActionDelete, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
			return err
		}
		if err := e.state.Set(rel, stateFromVersion(version)); err != nil {
			e.reportRecord(syncmodel.SyncActionDelete, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
			return err
		}
		e.reportRecord(syncmodel.SyncActionDelete, syncmodel.SyncRecordStatusSuccess, rel, nil, time.Since(started))
		return nil
	case syncmodel.EntryFile:
		if hasKnown && known.VersionID == version.VersionID {
			e.reportRecord(syncmodel.SyncActionSkip, syncmodel.SyncRecordStatusSuccess, rel, nil, time.Since(started))
			return nil
		}
		if hasLocalDivergence(target, known) {
			conflict := conflictLocalPath(target, e.cfg.DeviceID)
			e.log.Warn("local edit conflict preserved", "path", rel, "conflict", conflict)
			if err := fileutil.EnsureParent(conflict); err != nil {
				return err
			}
			if err := os.Rename(target, conflict); err != nil && !errors.Is(err, os.ErrNotExist) {
				e.reportRecord(syncmodel.SyncActionConflict, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
				return err
			}
			e.reportRecord(syncmodel.SyncActionConflict, syncmodel.SyncRecordStatusSuccess, rel, nil, time.Since(started))
		}
		if err := e.downloadVersion(ctx, target, version); err != nil {
			e.reportRecord(syncmodel.SyncActionDownload, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
			return err
		}
		if err := e.state.Set(rel, stateFromVersion(version)); err != nil {
			e.reportRecord(syncmodel.SyncActionDownload, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
			return err
		}
		e.reportRecord(syncmodel.SyncActionDownload, syncmodel.SyncRecordStatusSuccess, rel, nil, time.Since(started))
		return nil
	default:
		err := fmt.Errorf("unknown version state %q", version.State)
		e.reportRecord(syncmodel.SyncActionDownload, syncmodel.SyncRecordStatusFailed, rel, err, time.Since(started))
		return err
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
	root := e.cfg.LocalRoot
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

func localRootFor(storageRoot, root, spaceID string) string {
	if storageRoot == "" {
		storageRoot = root
	}
	storageRootAbs, storageErr := filepath.Abs(storageRoot)
	rootAbs, rootErr := filepath.Abs(root)
	if storageErr == nil && rootErr == nil {
		if fileutil.PathWithin(storageRootAbs, rootAbs) {
			return rootAbs
		}
		storageRoot = storageRootAbs
		root = rootAbs
	}
	if spaceID == "" {
		spaceID = "default"
	}
	sum := sha256.Sum256([]byte(root))
	base := safeStateName(filepath.Base(root))
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "root"
	}
	return filepath.Join(storageRoot, safeStateName(spaceID), hex.EncodeToString(sum[:])[:12]+"-"+base)
}

func defaultDeviceID(deviceID, root, storageRoot, serverURL string) string {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID != "" {
		return deviceID
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "device"
	}
	sum := sha256.Sum256([]byte(serverURL + "\x00" + root + "\x00" + storageRoot))
	return safeStateName(host) + "-" + hex.EncodeToString(sum[:])[:8]
}

func discoverRootFolders() ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	add := func(path string) {
		if path == "" {
			return
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return
		}
		info, err := os.Stat(abs)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return
		}
		if seen[abs] || shouldSkipDiscoveredDir(filepath.Base(abs)) {
			return
		}
		seen[abs] = true
		out = append(out, abs)
	}
	if cwd, err := os.Getwd(); err == nil {
		add(cwd)
	}
	if home, err := os.UserHomeDir(); err == nil {
		add(home)
	}
	sort.Strings(out)
	return out, nil
}

func discoverChildFolders(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if shouldSkipDiscoveredDir(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		abs, err := filepath.Abs(filepath.Join(root, name))
		if err != nil {
			continue
		}
		out = append(out, abs)
	}
	sort.Strings(out)
	return out, nil
}

func shouldSkipDiscoveredDir(name string) bool {
	if name == "" {
		return true
	}
	if strings.HasPrefix(name, ".") {
		return true
	}
	switch name {
	case "Applications", "Library", "Movies", "Music", "Public", "go", "node_modules":
		return true
	default:
		return false
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func clientStateRoot() string {
	if home, err := os.UserHomeDir(); err == nil {
		return home
	}
	if cwd, err := os.Getwd(); err == nil {
		return cwd
	}
	return "."
}

func safeStateName(v string) string {
	out := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, v)
	out = strings.Trim(out, "-")
	if out == "" {
		return "default"
	}
	return out
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
