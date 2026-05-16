package server

import (
	"crypto/sha256"
	"crypto/subtle"
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

const (
	defaultAdminUser = "admin"
	defaultAdminPass = "admin123"
)

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
			Accounts:      map[string]*syncmodel.Account{},
			Spaces:        map[string]*syncmodel.SyncSpace{},
			ClientFolders: map[string]*syncmodel.ClientFolder{},
			Files:         map[string]*syncmodel.FileEntry{},
			Versions:      map[string][]syncmodel.FileVersion{},
			ChunkRefs:     map[string]int{},
			AccountChunks: map[string]bool{},
			DeviceSeq:     map[string]int64{},
			Operations:    map[string]syncmodel.CommitResponse{},
		},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	if err := s.ensureBootstrap(); err != nil {
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
	if s.state.Accounts == nil {
		s.state.Accounts = map[string]*syncmodel.Account{}
	}
	if s.state.Spaces == nil {
		s.state.Spaces = map[string]*syncmodel.SyncSpace{}
	}
	if s.state.ClientFolders == nil {
		s.state.ClientFolders = map[string]*syncmodel.ClientFolder{}
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
	if s.state.AccountChunks == nil {
		s.state.AccountChunks = map[string]bool{}
	}
	if s.state.DeviceSeq == nil {
		s.state.DeviceSeq = map[string]int64{}
	}
	if s.state.Operations == nil {
		s.state.Operations = map[string]syncmodel.CommitResponse{}
	}
	return nil
}

func (s *Store) ensureBootstrap() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.Accounts) == 0 {
		now := time.Now().Unix()
		account := &syncmodel.Account{
			ID:            fileutil.NewID(),
			Username:      defaultAdminUser,
			DisplayName:   "Administrator",
			PasswordHash:  HashSecret(defaultAdminPass),
			SyncTokenHash: HashSecret(fileutil.NewID()),
			IsAdmin:       true,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		s.state.Accounts[account.ID] = account
		space := &syncmodel.SyncSpace{
			ID:        "default",
			AccountID: account.ID,
			Name:      "default",
			Active:    true,
			CreatedAt: now,
			UpdatedAt: now,
		}
		s.state.Spaces[spaceKey(account.ID, space.ID)] = space
		s.migrateLegacyScopeLocked(account.ID, space.ID)
		return s.saveLocked()
	}
	changed := s.normalizeAccountsLocked()
	if s.normalizeFoldersLocked() {
		changed = true
	}
	var firstAccount *syncmodel.Account
	for _, account := range s.state.Accounts {
		firstAccount = account
		break
	}
	if firstAccount == nil {
		return nil
	}
	if s.state.Spaces[spaceKey(firstAccount.ID, "default")] == nil {
		now := time.Now().Unix()
		s.state.Spaces[spaceKey(firstAccount.ID, "default")] = &syncmodel.SyncSpace{
			ID:        "default",
			AccountID: firstAccount.ID,
			Name:      "default",
			Active:    true,
			CreatedAt: now,
			UpdatedAt: now,
		}
		changed = true
	}
	if s.migrateLegacyScopeLocked(firstAccount.ID, "default") {
		changed = true
	}
	if changed {
		return s.saveLocked()
	}
	return nil
}

func (s *Store) normalizeAccountsLocked() bool {
	changed := false
	for _, account := range s.state.Accounts {
		if account.DisplayName == "" {
			account.DisplayName = account.Username
			changed = true
		}
		normalizedEmail := strings.ToLower(strings.TrimSpace(account.Email))
		if account.Email != normalizedEmail {
			account.Email = normalizedEmail
			changed = true
		}
	}
	return changed
}

func (s *Store) normalizeFoldersLocked() bool {
	changed := false
	for _, folder := range s.state.ClientFolders {
		if folder.Status == "" {
			folder.Status = syncmodel.FolderPending
			changed = true
		}
	}
	return changed
}

func (s *Store) migrateLegacyScopeLocked(accountID, spaceID string) bool {
	changed := false
	newFiles := map[string]*syncmodel.FileEntry{}
	for key, entry := range s.state.Files {
		if entry.AccountID == "" {
			entry.AccountID = accountID
			entry.SpaceID = spaceID
			changed = true
		}
		newFiles[fileKey(entry.AccountID, entry.SpaceID, entry.Path)] = entry
		if key != fileKey(entry.AccountID, entry.SpaceID, entry.Path) {
			changed = true
		}
	}
	s.state.Files = newFiles
	for fileID, versions := range s.state.Versions {
		for i := range versions {
			if versions[i].AccountID == "" {
				versions[i].AccountID = accountID
				versions[i].SpaceID = spaceID
				changed = true
			}
		}
		s.state.Versions[fileID] = versions
	}
	for i := range s.state.Events {
		if s.state.Events[i].AccountID == "" {
			s.state.Events[i].AccountID = accountID
			s.state.Events[i].SpaceID = spaceID
			changed = true
		}
	}
	for hash := range s.state.ChunkRefs {
		key := accountChunkKey(accountID, hash)
		if !s.state.AccountChunks[key] {
			s.state.AccountChunks[key] = true
			changed = true
		}
	}
	return changed
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

func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func VerifySecret(secret, hash string) bool {
	if secret == "" || hash == "" {
		return false
	}
	got := HashSecret(secret)
	return subtle.ConstantTimeCompare([]byte(got), []byte(hash)) == 1
}

func (s *Store) AuthenticateSyncToken(token string) (*syncmodel.Account, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, account := range s.state.Accounts {
		if account.Disabled {
			continue
		}
		if VerifySecret(token, account.SyncTokenHash) {
			copy := *account
			return &copy, true
		}
	}
	return nil, false
}

func (s *Store) AuthenticatePassword(identifier, password string) (*syncmodel.Account, bool) {
	identifier = strings.ToLower(strings.TrimSpace(identifier))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, account := range s.state.Accounts {
		if account.Disabled {
			continue
		}
		username := strings.ToLower(account.Username)
		email := strings.ToLower(account.Email)
		if username != identifier && (email == "" || email != identifier) {
			continue
		}
		if VerifySecret(password, account.PasswordHash) {
			copy := *account
			return &copy, true
		}
	}
	return nil, false
}

func (s *Store) GetAccount(id string) (*syncmodel.Account, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[id]
	if account == nil || account.Disabled {
		return nil, false
	}
	copy := *account
	return &copy, true
}

func (s *Store) AccountByUsername(username string) (*syncmodel.Account, bool) {
	username = strings.ToLower(strings.TrimSpace(username))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, account := range s.state.Accounts {
		if strings.ToLower(account.Username) == username {
			copy := *account
			return &copy, true
		}
	}
	return nil, false
}

func (s *Store) ListAccounts() []syncmodel.Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	accounts := make([]syncmodel.Account, 0, len(s.state.Accounts))
	for _, account := range s.state.Accounts {
		copy := *account
		copy.PasswordHash = ""
		copy.SyncTokenHash = ""
		accounts = append(accounts, copy)
	}
	sort.Slice(accounts, func(i, j int) bool {
		return accounts[i].Username < accounts[j].Username
	})
	return accounts
}

func (s *Store) CreateAccount(username, email, displayName, password string, admin bool) (syncmodel.Account, string, error) {
	username = strings.TrimSpace(username)
	email = strings.ToLower(strings.TrimSpace(email))
	displayName = strings.TrimSpace(displayName)
	if username == "" {
		return syncmodel.Account{}, "", errors.New("username is required")
	}
	if !validAccountName(username) {
		return syncmodel.Account{}, "", errors.New("username can only contain letters, numbers, dots, underscores, and hyphens")
	}
	if email != "" && !strings.Contains(email, "@") {
		return syncmodel.Account{}, "", errors.New("email is invalid")
	}
	if displayName == "" {
		displayName = username
	}
	if password == "" {
		return syncmodel.Account{}, "", errors.New("password is required")
	}
	if len(password) < 8 {
		return syncmodel.Account{}, "", errors.New("password must be at least 8 characters")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, account := range s.state.Accounts {
		if strings.EqualFold(account.Username, username) {
			return syncmodel.Account{}, "", fmt.Errorf("account %q already exists", username)
		}
		if email != "" && strings.EqualFold(account.Email, email) {
			return syncmodel.Account{}, "", fmt.Errorf("email %q is already registered", email)
		}
	}
	now := time.Now().Unix()
	token := fileutil.NewID() + fileutil.NewID()
	account := &syncmodel.Account{
		ID:            fileutil.NewID(),
		Username:      username,
		DisplayName:   displayName,
		Email:         email,
		PasswordHash:  HashSecret(password),
		SyncTokenHash: HashSecret(token),
		IsAdmin:       admin,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	s.state.Accounts[account.ID] = account
	space := &syncmodel.SyncSpace{
		ID:        "default",
		AccountID: account.ID,
		Name:      "default",
		Active:    true,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.state.Spaces[spaceKey(account.ID, space.ID)] = space
	if err := s.saveLocked(); err != nil {
		return syncmodel.Account{}, "", err
	}
	copy := *account
	copy.PasswordHash = ""
	copy.SyncTokenHash = ""
	return copy, token, nil
}

func (s *Store) ChangePassword(accountID, currentPassword, newPassword string) error {
	if len(newPassword) < 8 {
		return errors.New("new password must be at least 8 characters")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[accountID]
	if account == nil || account.Disabled {
		return errors.New("account not found")
	}
	if !VerifySecret(currentPassword, account.PasswordHash) {
		return errors.New("current password is incorrect")
	}
	account.PasswordHash = HashSecret(newPassword)
	account.UpdatedAt = time.Now().Unix()
	return s.saveLocked()
}

func (s *Store) ResetSyncToken(accountID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[accountID]
	if account == nil {
		return "", errors.New("account not found")
	}
	token := fileutil.NewID() + fileutil.NewID()
	account.SyncTokenHash = HashSecret(token)
	account.UpdatedAt = time.Now().Unix()
	if err := s.saveLocked(); err != nil {
		return "", err
	}
	return token, nil
}

func (s *Store) SetAccountDisabled(accountID string, disabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[accountID]
	if account == nil {
		return errors.New("account not found")
	}
	account.Disabled = disabled
	account.UpdatedAt = time.Now().Unix()
	return s.saveLocked()
}

func (s *Store) ListSpaces(accountID string) []syncmodel.SpaceSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []syncmodel.SpaceSummary{}
	for _, space := range s.state.Spaces {
		if space.AccountID != accountID {
			continue
		}
		summary := syncmodel.SpaceSummary{Space: *space}
		for _, entry := range s.state.Files {
			if entry.AccountID != accountID || entry.SpaceID != space.ID {
				continue
			}
			if entry.Deleted {
				summary.Deleted++
			} else {
				summary.FileCount++
			}
		}
		for _, folder := range s.state.ClientFolders {
			if folder.AccountID == accountID && folder.SpaceID == space.ID && folder.Status == syncmodel.FolderSelected {
				summary.Folders++
			}
		}
		out = append(out, summary)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Space.Name < out[j].Space.Name
	})
	return out
}

func (s *Store) ReportFolder(accountID string, req syncmodel.FolderReportRequest) (syncmodel.FolderReportResponse, error) {
	deviceID := strings.TrimSpace(req.DeviceID)
	rootPath := strings.TrimSpace(req.RootPath)
	hostname := strings.TrimSpace(req.Hostname)
	if deviceID == "" {
		return syncmodel.FolderReportResponse{}, errors.New("device_id is required")
	}
	if rootPath == "" {
		return syncmodel.FolderReportResponse{}, errors.New("root_path is required")
	}
	suggestedSpaceID := strings.TrimSpace(req.SuggestedSpaceID)
	if suggestedSpaceID == "" {
		suggestedSpaceID = "default"
	}
	now := time.Now().Unix()
	key := folderKey(accountID, deviceID, rootPath)

	s.mu.Lock()
	defer s.mu.Unlock()
	folder := s.state.ClientFolders[key]
	if folder == nil {
		folder = &syncmodel.ClientFolder{
			ID:               fileutil.NewID(),
			AccountID:        accountID,
			DeviceID:         deviceID,
			Hostname:         hostname,
			RootPath:         rootPath,
			SuggestedSpaceID: suggestedSpaceID,
			Status:           syncmodel.FolderPending,
			CreatedAt:        now,
		}
		s.state.ClientFolders[key] = folder
	} else {
		folder.Hostname = hostname
		folder.SuggestedSpaceID = suggestedSpaceID
	}
	folder.LastSeenAt = now
	folder.UpdatedAt = now
	var space *syncmodel.SyncSpace
	if folder.Status == syncmodel.FolderSelected && folder.SpaceID != "" {
		if selected := s.state.Spaces[spaceKey(accountID, folder.SpaceID)]; selected != nil && selected.Active {
			copy := *selected
			space = &copy
		}
	}
	if err := s.saveLocked(); err != nil {
		return syncmodel.FolderReportResponse{}, err
	}
	return syncmodel.FolderReportResponse{
		Folder:   *folder,
		Space:    space,
		Selected: space != nil,
	}, nil
}

func (s *Store) ListFolders(accountID string) []syncmodel.ClientFolder {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []syncmodel.ClientFolder{}
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID != accountID {
			continue
		}
		out = append(out, *folder)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Status != out[j].Status {
			return out[i].Status < out[j].Status
		}
		if out[i].DeviceID != out[j].DeviceID {
			return out[i].DeviceID < out[j].DeviceID
		}
		return out[i].RootPath < out[j].RootPath
	})
	return out
}

func (s *Store) SelectFolder(accountID, folderID, spaceID string) error {
	if folderID == "" {
		return errors.New("folder_id is required")
	}
	if spaceID == "" {
		return errors.New("space_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	space := s.state.Spaces[spaceKey(accountID, spaceID)]
	if space == nil || !space.Active {
		return errors.New("sync space not found")
	}
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID != accountID || folder.ID != folderID {
			continue
		}
		folder.SpaceID = spaceID
		folder.Status = syncmodel.FolderSelected
		folder.UpdatedAt = time.Now().Unix()
		return s.saveLocked()
	}
	return errors.New("client folder not found")
}

func (s *Store) DisableFolder(accountID, folderID string) error {
	if folderID == "" {
		return errors.New("folder_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID != accountID || folder.ID != folderID {
			continue
		}
		folder.Status = syncmodel.FolderDisabled
		folder.UpdatedAt = time.Now().Unix()
		return s.saveLocked()
	}
	return errors.New("client folder not found")
}

func (s *Store) FolderSelected(accountID, deviceID, rootPath, spaceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	folder := s.state.ClientFolders[folderKey(accountID, deviceID, rootPath)]
	return folder != nil && folder.Status == syncmodel.FolderSelected && folder.SpaceID == spaceID
}

func (s *Store) CreateSpace(accountID, name, description string) (syncmodel.SyncSpace, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return syncmodel.SyncSpace{}, errors.New("space name is required")
	}
	id, err := fileutil.CleanRel(name)
	if err != nil || strings.Contains(id, "/") {
		id = fileutil.NewID()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.state.Spaces[spaceKey(accountID, id)] != nil {
		id = fileutil.NewID()
	}
	now := time.Now().Unix()
	space := &syncmodel.SyncSpace{
		ID:          id,
		AccountID:   accountID,
		Name:        name,
		Description: strings.TrimSpace(description),
		Active:      true,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.state.Spaces[spaceKey(accountID, id)] = space
	if err := s.saveLocked(); err != nil {
		return syncmodel.SyncSpace{}, err
	}
	return *space, nil
}

func (s *Store) GetSpace(accountID, spaceID string) (*syncmodel.SyncSpace, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	space := s.state.Spaces[spaceKey(accountID, spaceID)]
	if space == nil || !space.Active {
		return nil, false
	}
	copy := *space
	return &copy, true
}

func (s *Store) SetSpaceActive(accountID, spaceID string, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	space := s.state.Spaces[spaceKey(accountID, spaceID)]
	if space == nil {
		return errors.New("space not found")
	}
	space.Active = active
	space.UpdatedAt = time.Now().Unix()
	return s.saveLocked()
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

func (s *Store) CheckChunks(accountID string, chunks []string) []string {
	missing := make([]string, 0)
	for _, hash := range chunks {
		if !s.HasAccountChunk(accountID, hash) {
			missing = append(missing, hash)
		}
	}
	return missing
}

func (s *Store) GrantAccountChunk(accountID, hash string) error {
	if !s.HasChunk(hash) {
		return fmt.Errorf("missing chunk %s", hash)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.AccountChunks[accountChunkKey(accountID, hash)] = true
	return s.saveLocked()
}

func (s *Store) HasAccountChunk(accountID, hash string) bool {
	if !s.HasChunk(hash) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.AccountChunks[accountChunkKey(accountID, hash)]
}

func (s *Store) ListFiles(accountID, spaceID string) []syncmodel.FileEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	files := make([]syncmodel.FileEntry, 0, len(s.state.Files))
	for _, entry := range s.state.Files {
		if entry.AccountID != accountID || entry.SpaceID != spaceID {
			continue
		}
		files = append(files, *entry)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files
}

func (s *Store) EventsAfter(accountID, spaceID string, after int64) []syncmodel.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []syncmodel.Event
	for _, event := range s.state.Events {
		if event.AccountID == accountID && event.SpaceID == spaceID && event.Seq > after {
			events = append(events, event)
		}
	}
	return events
}

func (s *Store) Commit(accountID, spaceID string, req syncmodel.CommitRequest) (syncmodel.CommitResponse, error) {
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
	if _, ok := s.GetSpace(accountID, spaceID); !ok {
		return syncmodel.CommitResponse{}, errors.New("sync space not found")
	}
	path, err := fileutil.CleanRel(manifest.Path)
	if err != nil {
		return syncmodel.CommitResponse{}, err
	}
	if err := s.verifyManifest(accountID, manifest); err != nil {
		return syncmodel.CommitResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	opKey := operationKey(accountID, spaceID, req.OperationID)
	if existing, ok := s.state.Operations[opKey]; ok {
		return existing, nil
	}

	now := time.Now().Unix()
	key := fileKey(accountID, spaceID, path)
	entry := s.state.Files[key]
	if entry == nil {
		entry = &syncmodel.FileEntry{
			AccountID: accountID,
			SpaceID:   spaceID,
			FileID:    fileutil.NewID(),
			Path:      path,
		}
		s.state.Files[key] = entry
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
			AccountID: accountID,
			SpaceID:   spaceID,
			FileID:    fileutil.NewID(),
			Path:      conflictPath,
		}
		s.state.Files[fileKey(accountID, spaceID, conflictPath)] = entry
		path = conflictPath
	}

	version := syncmodel.FileVersion{
		AccountID:   accountID,
		SpaceID:     spaceID,
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
		s.state.AccountChunks[accountChunkKey(accountID, chunk.Hash)] = true
	}
	s.appendEventLocked(version)

	resp := syncmodel.CommitResponse{
		Status:       "ok",
		Entry:        *entry,
		Version:      version,
		Conflict:     conflict,
		ConflictPath: conflictPath,
	}
	s.state.Operations[opKey] = resp
	if err := s.saveLocked(); err != nil {
		return syncmodel.CommitResponse{}, err
	}
	return resp, nil
}

func (s *Store) Delete(accountID, spaceID string, req syncmodel.DeleteRequest) (syncmodel.CommitResponse, error) {
	if req.OperationID == "" {
		return syncmodel.CommitResponse{}, errors.New("operation_id is required")
	}
	if req.DeviceID == "" {
		return syncmodel.CommitResponse{}, errors.New("device_id is required")
	}
	if req.Path == "" {
		return syncmodel.CommitResponse{}, errors.New("path is required")
	}
	if _, ok := s.GetSpace(accountID, spaceID); !ok {
		return syncmodel.CommitResponse{}, errors.New("sync space not found")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	opKey := operationKey(accountID, spaceID, req.OperationID)
	if existing, ok := s.state.Operations[opKey]; ok {
		return existing, nil
	}

	path, err := fileutil.CleanRel(req.Path)
	if err != nil {
		return syncmodel.CommitResponse{}, err
	}
	entry := s.state.Files[fileKey(accountID, spaceID, path)]
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
		s.state.Operations[opKey] = resp
		if err := s.saveLocked(); err != nil {
			return syncmodel.CommitResponse{}, err
		}
		return resp, nil
	}

	now := time.Now().Unix()
	version := syncmodel.FileVersion{
		AccountID:   accountID,
		SpaceID:     spaceID,
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
	s.state.Operations[opKey] = resp
	if err := s.saveLocked(); err != nil {
		return syncmodel.CommitResponse{}, err
	}
	return resp, nil
}

func (s *Store) verifyManifest(accountID string, manifest syncmodel.Manifest) error {
	if len(manifest.Chunks) == 0 {
		if manifest.Size != 0 {
			return errors.New("empty chunk list with non-zero size")
		}
		if manifest.Hash != fileutil.HashBytes(nil) {
			return errors.New("empty file hash mismatch")
		}
		s.mu.Lock()
		s.state.AccountChunks[accountChunkKey(accountID, manifest.Hash)] = true
		s.mu.Unlock()
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
		if !s.HasAccountChunk(accountID, chunk.Hash) {
			return fmt.Errorf("chunk %s is not available to this account", chunk.Hash)
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
		AccountID: version.AccountID,
		SpaceID:   version.SpaceID,
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
	for i := 2; s.conflictPathExistsLocked(path, candidate); i++ {
		candidate = fmt.Sprintf("%s (conflict %d from %s at %s)%s", stem, i, safeDevice, time.Unix(ts, 0).Format("20060102-150405"), ext)
	}
	return filepath.ToSlash(candidate)
}

func (s *Store) conflictPathExistsLocked(_, candidate string) bool {
	for _, entry := range s.state.Files {
		if entry.Path == candidate {
			return true
		}
	}
	return false
}

func fileKey(accountID, spaceID, path string) string {
	return accountID + "\x00" + spaceID + "\x00" + path
}

func operationKey(accountID, spaceID, operationID string) string {
	return accountID + "\x00" + spaceID + "\x00" + operationID
}

func accountChunkKey(accountID, hash string) string {
	return accountID + "\x00" + hash
}

func spaceKey(accountID, spaceID string) string {
	return accountID + "\x00" + spaceID
}

func folderKey(accountID, deviceID, rootPath string) string {
	return accountID + "\x00" + strings.TrimSpace(deviceID) + "\x00" + strings.TrimSpace(rootPath)
}

func validAccountName(v string) bool {
	if len(v) < 3 || len(v) > 64 {
		return false
	}
	for _, r := range v {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
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
