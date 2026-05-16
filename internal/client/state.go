package client

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"xcloud/internal/fileutil"
	"xcloud/internal/syncmodel"
)

type State struct {
	mu   sync.Mutex
	path string
	Data StateData `json:"data"`
}

type StateData struct {
	Files        map[string]syncmodel.LocalFileState `json:"files"`
	LastEventSeq int64                               `json:"last_event_seq"`
	SpaceID      string                              `json:"space_id,omitempty"`
	ReportedDirs map[string]bool                     `json:"reported_dirs,omitempty"`
}

func OpenState(path string) (*State, error) {
	st := &State{
		path: path,
		Data: StateData{
			Files:        map[string]syncmodel.LocalFileState{},
			ReportedDirs: map[string]bool{},
		},
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &st.Data); err != nil {
			return nil, err
		}
	}
	if st.Data.Files == nil {
		st.Data.Files = map[string]syncmodel.LocalFileState{}
	}
	if st.Data.ReportedDirs == nil {
		st.Data.ReportedDirs = map[string]bool{}
	}
	return st, nil
}

func (s *State) Get(path string) (syncmodel.LocalFileState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.Data.Files[path]
	return v, ok
}

func (s *State) Set(path string, value syncmodel.LocalFileState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Data.Files[path] = value
	return s.saveLocked()
}

func (s *State) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Data.Files, path)
	return s.saveLocked()
}

func (s *State) Snapshot() map[string]syncmodel.LocalFileState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]syncmodel.LocalFileState, len(s.Data.Files))
	for k, v := range s.Data.Files {
		out[k] = v
	}
	return out
}

func (s *State) EnsureSpace(spaceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Data.SpaceID == spaceID {
		return nil
	}
	if s.Data.SpaceID != "" && spaceID != "" {
		s.Data.Files = map[string]syncmodel.LocalFileState{}
		s.Data.LastEventSeq = 0
	}
	s.Data.SpaceID = spaceID
	return s.saveLocked()
}

func (s *State) SeedSpace(spaceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Data.SpaceID != "" || spaceID == "" {
		return nil
	}
	s.Data.SpaceID = spaceID
	return s.saveLocked()
}

func (s *State) DirReported(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Data.ReportedDirs[path]
}

func (s *State) MarkDirReported(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Data.ReportedDirs == nil {
		s.Data.ReportedDirs = map[string]bool{}
	}
	if s.Data.ReportedDirs[path] {
		return nil
	}
	s.Data.ReportedDirs[path] = true
	return s.saveLocked()
}

func (s *State) LastEventSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Data.LastEventSeq
}

func (s *State) SetLastEventSeq(seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.Data.LastEventSeq {
		s.Data.LastEventSeq = seq
	}
	return s.saveLocked()
}

func (s *State) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.Data, "", "  ")
	if err != nil {
		return err
	}
	return fileutil.AtomicWrite(s.path, func(f *os.File) error {
		_, err := f.Write(b)
		return err
	})
}
