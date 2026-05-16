package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"xcloud/internal/fileutil"
	"xcloud/internal/syncmodel"
)

const maxEvents = 10000

type Store struct {
	mu        sync.Mutex
	root      string
	state     syncmodel.ServerState
	statePath string
}

func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(root, "chunks"), 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		root:      root,
		statePath: filepath.Join(root, "metadata.json"),
		state: syncmodel.ServerState{
			Files:      map[string]*syncmodel.FileEntry{},
			Versions:   map[string][]syncmodel.FileVersion{},
			ChunkRefs:  map[string]int{},
			DeviceSeq:  map[string]int64{},
			Operations: map[string]syncmodel.CommitResponse{},
		},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	b, err := os.ReadFile(s.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, &s.state); err != nil {
		return err
	}
	if s.state.Files == nil {
		s.state.Files = map[string]*syncmodel.FileEntry{}
	}
	if s.state.Versions == nil {
		s.state.Versions = map[string][]syncmodel.FileVersion{}
	}
	if s.state.ChunkRefs == nil {
		s.state.ChunkRefs = map[string]int{}
	}
	if s.state.DeviceSeq == nil {
		s.state.DeviceSeq = map[string]int64{}
	}
	if s.state.Operations == nil {
		s.state.Operations = map[string]syncmodel.CommitResponse{}
	}
	return nil
}

func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	return fileutil.AtomicWrite(s.statePath, func(f *os.File) error {
		_, err := f.Write(b)
		return err
	})
}

func (s *Store) ChunkPath(hash string) (string, error) {
	if !isHash(hash) {
		return "", fmt.Errorf("invalid chunk hash %q", hash)
	}
	return filepath.Join(s.root, "chunks", hash[:2], hash[2:4], hash), nil
}

func (s *Store) HasChunk(hash string) bool {
	p, err := s.ChunkPath(hash)
	if err != nil {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}

func (s *Store) PutChunk(hash string, data []byte) error {
	if fileutil.HashBytes(data) != hash {
		return errors.New("chunk hash mismatch")
	}
	p, err := s.ChunkPath(hash)
	if err != nil {
		return err
	}
	if s.HasChunk(hash) {
		return nil
	}
	return fileutil.AtomicWrite(p, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
}

func (s *Store) CheckChunks(chunks []string) []string {
	missing := make([]string, 0)
	for _, hash := range chunks {
		if !s.HasChunk(hash) {
			missing = append(missing, hash)
		}
	}
	return missing
}

func (s *Store) ListFiles() []syncmodel.FileEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	files := make([]syncmodel.FileEntry, 0, len(s.state.Files))
	for _, entry := range s.state.Files {
		files = append(files, *entry)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files
}

func (s *Store) EventsAfter(after int64) []syncmodel.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []syncmodel.Event
	for _, event := range s.state.Events {
		if event.Seq > after {
			events = append(events, event)
		}
	}
	return events
}

func (s *Store) Commit(req syncmodel.CommitRequest) (syncmodel.CommitResponse, error) {
	if req.OperationID == "" {
		return syncmodel.CommitResponse{}, errors.New("operation_id is required")
	}
	if req.DeviceID == "" {
		return syncmodel.CommitResponse{}, errors.New("device_id is required")
	}
	manifest := req.Manifest
	if manifest.Path == "" || manifest.Hash == "" {
		return syncmodel.CommitResponse{}, errors.New("manifest path and hash are required")
	}
	if !isHash(manifest.Hash) {
		return syncmodel.CommitResponse{}, errors.New("invalid file hash")
	}
	path, err := fileutil.CleanRel(manifest.Path)
	if err != nil {
		return syncmodel.CommitResponse{}, err
	}
	if err := s.verifyManifest(manifest); err != nil {
		return syncmodel.CommitResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.state.Operations[req.OperationID]; ok {
		return existing, nil
	}

	now := time.Now().Unix()
	entry := s.state.Files[path]
	if entry == nil {
		entry = &syncmodel.FileEntry{
			FileID: fileutil.NewID(),
			Path:   path,
		}
		s.state.Files[path] = entry
	}
	if manifest.FileID != "" && entry.FileID != manifest.FileID {
		return syncmodel.CommitResponse{}, fmt.Errorf("file_id mismatch for %s", path)
	}

	conflict := false
	conflictPath := ""
	if entry.Current != nil && manifest.BaseVersion != entry.Current.VersionID {
		conflict = true
		conflictPath = s.nextConflictPathLocked(path, req.DeviceID, now)
		entry = &syncmodel.FileEntry{
			FileID: fileutil.NewID(),
			Path:   conflictPath,
		}
		s.state.Files[conflictPath] = entry
		path = conflictPath
	}

	version := syncmodel.FileVersion{
		FileID:      entry.FileID,
		Path:        path,
		VersionID:   fileutil.NewID(),
		BaseVersion: manifest.BaseVersion,
		State:       syncmodel.EntryFile,
		Size:        manifest.Size,
		Hash:        manifest.Hash,
		Chunks:      append([]syncmodel.ChunkRef(nil), manifest.Chunks...),
		ModTimeUnix: manifest.ModTimeUnix,
		DeviceID:    req.DeviceID,
		CreatedAt:   now,
	}
	entry.Current = &version
	entry.Deleted = false
	entry.LatestVersion = version.VersionID
	entry.UpdatedAt = now
	s.state.Versions[entry.FileID] = append(s.state.Versions[entry.FileID], version)
	for _, chunk := range version.Chunks {
		s.state.ChunkRefs[chunk.Hash]++
	}
	s.appendEventLocked(version)

	resp := syncmodel.CommitResponse{
		Status:       "ok",
		Entry:        *entry,
		Version:      version,
		Conflict:     conflict,
		ConflictPath: conflictPath,
	}
	s.state.Operations[req.OperationID] = resp
	if err := s.saveLocked(); err != nil {
		return syncmodel.CommitResponse{}, err
	}
	return resp, nil
}

func (s *Store) Delete(req syncmodel.DeleteRequest) (syncmodel.CommitResponse, error) {
	if req.OperationID == "" {
		return syncmodel.CommitResponse{}, errors.New("operation_id is required")
	}
	if req.DeviceID == "" {
		return syncmodel.CommitResponse{}, errors.New("device_id is required")
	}
	if req.Path == "" {
		return syncmodel.CommitResponse{}, errors.New("path is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.state.Operations[req.OperationID]; ok {
		return existing, nil
	}

	path, err := fileutil.CleanRel(req.Path)
	if err != nil {
		return syncmodel.CommitResponse{}, err
	}
	entry := s.state.Files[path]
	if entry == nil {
		return syncmodel.CommitResponse{}, fmt.Errorf("path not found: %s", path)
	}
	if req.FileID != "" && req.FileID != entry.FileID {
		return syncmodel.CommitResponse{}, fmt.Errorf("file_id mismatch for %s", path)
	}
	if entry.Current != nil && req.BaseVersion != entry.Current.VersionID {
		resp := syncmodel.CommitResponse{
			Status:         "conflict",
			Entry:          *entry,
			Conflict:       true,
			CurrentVersion: entry.Current,
		}
		s.state.Operations[req.OperationID] = resp
		if err := s.saveLocked(); err != nil {
			return syncmodel.CommitResponse{}, err
		}
		return resp, nil
	}

	now := time.Now().Unix()
	version := syncmodel.FileVersion{
		FileID:      entry.FileID,
		Path:        path,
		VersionID:   fileutil.NewID(),
		BaseVersion: req.BaseVersion,
		State:       syncmodel.EntryDeleted,
		DeletedAt:   now,
		DeviceID:    req.DeviceID,
		CreatedAt:   now,
	}
	entry.Current = &version
	entry.Deleted = true
	entry.LatestVersion = version.VersionID
	entry.UpdatedAt = now
	s.state.Versions[entry.FileID] = append(s.state.Versions[entry.FileID], version)
	s.appendEventLocked(version)

	resp := syncmodel.CommitResponse{
		Status:  "ok",
		Entry:   *entry,
		Version: version,
	}
	s.state.Operations[req.OperationID] = resp
	if err := s.saveLocked(); err != nil {
		return syncmodel.CommitResponse{}, err
	}
	return resp, nil
}

func (s *Store) verifyManifest(manifest syncmodel.Manifest) error {
	if len(manifest.Chunks) == 0 {
		if manifest.Size != 0 {
			return errors.New("empty chunk list with non-zero size")
		}
		if manifest.Hash != fileutil.HashBytes(nil) {
			return errors.New("empty file hash mismatch")
		}
		return nil
	}
	h := sha256.New()
	var total int64
	for i, chunk := range manifest.Chunks {
		if chunk.Index != i {
			return fmt.Errorf("chunk index %d must be %d", chunk.Index, i)
		}
		if !isHash(chunk.Hash) {
			return fmt.Errorf("invalid chunk hash %q", chunk.Hash)
		}
		p, err := s.ChunkPath(chunk.Hash)
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("missing chunk %s", chunk.Hash)
		}
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(h, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != chunk.Size {
			return fmt.Errorf("chunk %s size mismatch", chunk.Hash)
		}
		total += n
	}
	if total != manifest.Size {
		return fmt.Errorf("file size mismatch: manifest=%d chunks=%d", manifest.Size, total)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != manifest.Hash {
		return errors.New("file hash mismatch")
	}
	return nil
}

func (s *Store) appendEventLocked(version syncmodel.FileVersion) {
	s.state.LastEventSeq++
	event := syncmodel.Event{
		Seq:       s.state.LastEventSeq,
		Path:      version.Path,
		FileID:    version.FileID,
		VersionID: version.VersionID,
		State:     version.State,
		DeviceID:  version.DeviceID,
		CreatedAt: version.CreatedAt,
		Version:   version,
	}
	s.state.Events = append(s.state.Events, event)
	if len(s.state.Events) > maxEvents {
		s.state.Events = s.state.Events[len(s.state.Events)-maxEvents:]
	}
}

func (s *Store) nextConflictPathLocked(path, deviceID string, ts int64) string {
	ext := filepath.Ext(path)
	stem := strings.TrimSuffix(path, ext)
	safeDevice := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, deviceID)
	base := fmt.Sprintf("%s (conflict from %s at %s)%s", stem, safeDevice, time.Unix(ts, 0).Format("20060102-150405"), ext)
	candidate := base
	for i := 2; s.state.Files[candidate] != nil; i++ {
		candidate = fmt.Sprintf("%s (conflict %d from %s at %s)%s", stem, i, safeDevice, time.Unix(ts, 0).Format("20060102-150405"), ext)
	}
	return filepath.ToSlash(candidate)
}

func isHash(v string) bool {
	if len(v) != 64 {
		return false
	}
	for _, r := range v {
		if r >= '0' && r <= '9' || r >= 'a' && r <= 'f' {
			continue
		}
		return false
	}
	return true
}
