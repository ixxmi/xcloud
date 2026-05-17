package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
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
	db        *sql.DB
}

func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(root, "chunks"), 0o755); err != nil {
		return nil, err
	}
	db, err := openSQLite(filepath.Join(root, "server.db"))
	if err != nil {
		return nil, err
	}
	s := &Store{
		root:      root,
		statePath: filepath.Join(root, "metadata.json"),
		db:        db,
		state: syncmodel.ServerState{
			Accounts:      map[string]*syncmodel.Account{},
			ClientTokens:  map[string]*syncmodel.ClientToken{},
			ClientDevices: map[string]*syncmodel.ClientDevice{},
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
		closeSQLite(db)
		return nil, err
	}
	if err := s.loadControlFromSQLite(); err != nil {
		closeSQLite(db)
		return nil, err
	}
	if err := s.ensureBootstrap(); err != nil {
		closeSQLite(db)
		return nil, err
	}
	if err := s.saveControlLocked(); err != nil {
		closeSQLite(db)
		return nil, err
	}
	return s, nil
}

func (s *Store) Root() string {
	return s.root
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) RuntimeConfig(fallback RuntimeConfig) (RuntimeConfig, error) {
	if s.db == nil {
		return fallback, nil
	}
	cfg, err := loadRuntimeConfigFromSQLite(s.db, fallback)
	if err != nil {
		return fallback, err
	}
	cfg.Path = fallback.Path
	return cfg, nil
}

func (s *Store) SaveRuntimeConfig(cfg RuntimeConfig) error {
	if s.db == nil {
		return nil
	}
	return saveRuntimeConfigToSQLite(s.db, cfg)
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
	if s.state.ClientTokens == nil {
		s.state.ClientTokens = map[string]*syncmodel.ClientToken{}
	}
	if s.state.ClientDevices == nil {
		s.state.ClientDevices = map[string]*syncmodel.ClientDevice{}
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

func (s *Store) loadControlFromSQLite() error {
	if s.db == nil {
		return nil
	}
	hasData, err := sqliteHasControlData(s.db)
	if err != nil {
		return err
	}
	if !hasData {
		return nil
	}
	return loadControlStateFromSQLite(s.db, &s.state)
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
			SyncSettings:  syncmodel.DefaultSyncSettings(),
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
	if s.normalizeDevicesLocked() {
		changed = true
	}
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
		normalizedSettings := syncmodel.NormalizeSyncSettings(account.SyncSettings)
		if !account.SyncSettings.RealtimeEnabled && account.SyncSettings.DebounceMillis == 0 && account.SyncSettings.IntervalSeconds == 0 {
			normalizedSettings = syncmodel.DefaultSyncSettings()
		}
		if account.SyncSettings != normalizedSettings {
			account.SyncSettings = normalizedSettings
			changed = true
		}
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
		if folder.Depth < 0 {
			folder.Depth = 0
			changed = true
		}
	}
	return changed
}

func (s *Store) normalizeDevicesLocked() bool {
	if s.state.ClientDevices == nil {
		s.state.ClientDevices = map[string]*syncmodel.ClientDevice{}
	}
	changed := false
	for _, token := range s.state.ClientTokens {
		if token.AccountID == "" || token.DeviceID == "" {
			continue
		}
		key := deviceKey(token.AccountID, token.DeviceID)
		device := s.state.ClientDevices[key]
		if device == nil {
			createdAt := token.CreatedAt
			if createdAt == 0 {
				createdAt = time.Now().Unix()
			}
			device = &syncmodel.ClientDevice{
				AccountID:  token.AccountID,
				DeviceID:   token.DeviceID,
				Hostname:   token.Hostname,
				CreatedAt:  createdAt,
				UpdatedAt:  createdAt,
				LastSeenAt: token.LastUsedAt,
			}
			s.state.ClientDevices[key] = device
			changed = true
			continue
		}
		if device.Hostname == "" && token.Hostname != "" {
			device.Hostname = token.Hostname
			changed = true
		}
		if token.LastUsedAt > device.LastSeenAt {
			device.LastSeenAt = token.LastUsedAt
			changed = true
		}
	}
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID == "" || folder.DeviceID == "" {
			continue
		}
		key := deviceKey(folder.AccountID, folder.DeviceID)
		device := s.state.ClientDevices[key]
		if device == nil {
			createdAt := folder.CreatedAt
			if createdAt == 0 {
				createdAt = folder.UpdatedAt
			}
			if createdAt == 0 {
				createdAt = time.Now().Unix()
			}
			device = &syncmodel.ClientDevice{
				AccountID:  folder.AccountID,
				DeviceID:   folder.DeviceID,
				Hostname:   folder.Hostname,
				CreatedAt:  createdAt,
				UpdatedAt:  createdAt,
				LastSeenAt: folder.LastSeenAt,
			}
			s.state.ClientDevices[key] = device
			changed = true
			continue
		}
		if device.Hostname == "" && folder.Hostname != "" {
			device.Hostname = folder.Hostname
			changed = true
		}
		if folder.LastSeenAt > device.LastSeenAt {
			device.LastSeenAt = folder.LastSeenAt
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
		if s.state.Events[i].RootPath == "" && s.state.Events[i].Version.RootPath != "" {
			s.state.Events[i].RootPath = s.state.Events[i].Version.RootPath
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
	if err := fileutil.AtomicWrite(s.statePath, func(f *os.File) error {
		_, err := f.Write(b)
		return err
	}); err != nil {
		return err
	}
	return s.saveControlLocked()
}

func (s *Store) saveControlLocked() error {
	if s.db == nil {
		return nil
	}
	return saveControlStateToSQLite(s.db, s.state)
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
	now := time.Now().Unix()
	changed := false
	for _, clientToken := range s.state.ClientTokens {
		if clientToken.Disabled {
			continue
		}
		if !VerifySecret(token, clientToken.TokenHash) {
			continue
		}
		account := s.state.Accounts[clientToken.AccountID]
		if account == nil || account.Disabled {
			continue
		}
		clientToken.LastUsedAt = now
		changed = true
		copy := *account
		if changed {
			_ = s.saveLocked()
		}
		return &copy, true
	}
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

func (s *Store) AccountForSyncToken(token string) (*syncmodel.Account, bool) {
	return s.AuthenticateSyncToken(token)
}

func (s *Store) IssueClientToken(identifier, password, deviceID, hostname, storageRoot string) (syncmodel.Account, string, string, error) {
	identifier = strings.ToLower(strings.TrimSpace(identifier))
	deviceID = strings.TrimSpace(deviceID)
	hostname = strings.TrimSpace(hostname)
	storageRoot = strings.TrimSpace(storageRoot)
	if storageRoot != "" && !isClientAbsolutePath(storageRoot) {
		return syncmodel.Account{}, "", "", errors.New("storage root must be absolute")
	}
	storageRoot = cleanClientPath(storageRoot)
	s.mu.Lock()
	defer s.mu.Unlock()
	var account *syncmodel.Account
	for _, candidate := range s.state.Accounts {
		if candidate.Disabled {
			continue
		}
		username := strings.ToLower(candidate.Username)
		email := strings.ToLower(candidate.Email)
		if username != identifier && (email == "" || email != identifier) {
			continue
		}
		if VerifySecret(password, candidate.PasswordHash) {
			account = candidate
			break
		}
	}
	if account == nil {
		return syncmodel.Account{}, "", "", errors.New("invalid account or password")
	}
	if s.state.ClientTokens == nil {
		s.state.ClientTokens = map[string]*syncmodel.ClientToken{}
	}
	if s.state.ClientDevices == nil {
		s.state.ClientDevices = map[string]*syncmodel.ClientDevice{}
	}
	now := time.Now().Unix()
	token := fileutil.NewID() + fileutil.NewID()
	clientToken := &syncmodel.ClientToken{
		ID:        fileutil.NewID(),
		AccountID: account.ID,
		DeviceID:  deviceID,
		Hostname:  hostname,
		TokenHash: HashSecret(token),
		CreatedAt: now,
	}
	s.state.ClientTokens[clientToken.ID] = clientToken
	effectiveStorageRoot := storageRoot
	if deviceID != "" {
		device := s.ensureClientDeviceLocked(account.ID, deviceID, hostname, now)
		if effectiveStorageRoot != "" && device.StorageRoot == "" {
			device.StorageRoot = effectiveStorageRoot
		}
		if device.StorageRoot != "" {
			effectiveStorageRoot = device.StorageRoot
		}
	}
	if err := s.saveLocked(); err != nil {
		return syncmodel.Account{}, "", "", err
	}
	copy := *account
	copy.PasswordHash = ""
	copy.SyncTokenHash = ""
	return copy, token, effectiveStorageRoot, nil
}

func (s *Store) SetAccountSyncEnabled(accountID string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[accountID]
	if account == nil || account.Disabled {
		return errors.New("account not found")
	}
	account.SyncEnabled = enabled
	account.UpdatedAt = time.Now().Unix()
	return s.saveLocked()
}

func (s *Store) SetSyncSettings(accountID string, settings syncmodel.SyncSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[accountID]
	if account == nil || account.Disabled {
		return errors.New("account not found")
	}
	account.SyncSettings = syncmodel.NormalizeSyncSettings(settings)
	account.UpdatedAt = time.Now().Unix()
	return s.saveLocked()
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
		SyncSettings:  syncmodel.DefaultSyncSettings(),
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
	if s.pruneTrashLocked(time.Now().Unix()) {
		defer func() { _ = s.saveLocked() }()
	}
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
				if entry.Current != nil && entry.Current.DeletedAt > 0 {
					summary.Trash++
				}
			} else {
				summary.FileCount++
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
	parentPath := strings.TrimSpace(req.ParentPath)
	hostname := strings.TrimSpace(req.Hostname)
	storageRoot := strings.TrimSpace(req.StorageRoot)
	if deviceID == "" {
		return syncmodel.FolderReportResponse{}, errors.New("device_id is required")
	}
	if rootPath == "" {
		return syncmodel.FolderReportResponse{}, errors.New("root_path is required")
	}
	if storageRoot != "" && !isClientAbsolutePath(storageRoot) {
		return syncmodel.FolderReportResponse{}, errors.New("storage_root must be absolute")
	}
	storageRoot = cleanClientPath(storageRoot)
	suggestedSpaceID := strings.TrimSpace(req.SuggestedSpaceID)
	if suggestedSpaceID == "" {
		suggestedSpaceID = "default"
	}
	depth := req.Depth
	if depth < 0 {
		depth = 0
	}
	now := time.Now().Unix()
	key := folderKey(accountID, deviceID, rootPath)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.ClientDevices == nil {
		s.state.ClientDevices = map[string]*syncmodel.ClientDevice{}
	}
	device := s.ensureClientDeviceLocked(accountID, deviceID, hostname, now)
	if storageRoot != "" && device.StorageRoot == "" {
		device.StorageRoot = storageRoot
		device.UpdatedAt = now
	}
	folder := s.state.ClientFolders[key]
	if folder == nil {
		folder = &syncmodel.ClientFolder{
			ID:               fileutil.NewID(),
			AccountID:        accountID,
			DeviceID:         deviceID,
			Hostname:         hostname,
			RootPath:         rootPath,
			ParentPath:       parentPath,
			Depth:            depth,
			SuggestedSpaceID: suggestedSpaceID,
			Status:           syncmodel.FolderPending,
			CreatedAt:        now,
		}
		s.state.ClientFolders[key] = folder
	} else {
		folder.Hostname = hostname
		folder.ParentPath = parentPath
		folder.Depth = depth
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

func (s *Store) ListClientDevices(accountID string) []syncmodel.ClientDevice {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []syncmodel.ClientDevice{}
	for _, device := range s.state.ClientDevices {
		if device.AccountID != accountID {
			continue
		}
		out = append(out, *device)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastSeenAt != out[j].LastSeenAt {
			return out[i].LastSeenAt > out[j].LastSeenAt
		}
		return out[i].DeviceID < out[j].DeviceID
	})
	return out
}

func (s *Store) FolderStatus(accountID, deviceID string) syncmodel.FolderStatusResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := syncmodel.FolderStatusResponse{Settings: syncmodel.DefaultSyncSettings()}
	if account := s.state.Accounts[accountID]; account != nil {
		resp.Settings = syncmodel.NormalizeSyncSettings(account.SyncSettings)
	}
	if device := s.state.ClientDevices[deviceKey(accountID, deviceID)]; device != nil {
		resp.StorageRoot = device.StorageRoot
	}
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID != accountID {
			continue
		}
		if deviceID != "" && folder.DeviceID != deviceID {
			continue
		}
		if folder.ChildrenRequested && !folder.ChildrenReported {
			resp.Requests = append(resp.Requests, syncmodel.FolderDiscoveryRequest{
				RootPath: folder.RootPath,
				Depth:    folder.Depth + 1,
			})
		}
		if folder.Status == syncmodel.FolderSelected && folder.SpaceID != "" {
			resp.Selected = append(resp.Selected, *folder)
		}
	}
	sort.Slice(resp.Requests, func(i, j int) bool {
		return resp.Requests[i].RootPath < resp.Requests[j].RootPath
	})
	sort.Slice(resp.Selected, func(i, j int) bool {
		return resp.Selected[i].RootPath < resp.Selected[j].RootPath
	})
	return resp
}

func (s *Store) ClientStatus(accountID, deviceID string) syncmodel.ClientStatusResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	account := s.state.Accounts[accountID]
	resp := syncmodel.ClientStatusResponse{
		SpaceID:  "default",
		Settings: syncmodel.DefaultSyncSettings(),
	}
	if account == nil {
		return resp
	}
	resp.Account = accountProfile(*account)
	resp.SyncEnabled = account.SyncEnabled
	resp.Settings = syncmodel.NormalizeSyncSettings(account.SyncSettings)
	if deviceID != "" {
		now := time.Now().Unix()
		if device := s.state.ClientDevices[deviceKey(accountID, deviceID)]; device != nil {
			device.LastSeenAt = now
			device.UpdatedAt = now
			resp.StorageRoot = device.StorageRoot
			_ = s.saveLocked()
		}
	} else if device := s.state.ClientDevices[deviceKey(accountID, deviceID)]; device != nil {
		resp.StorageRoot = device.StorageRoot
	}
	for _, space := range s.state.Spaces {
		if space.AccountID == accountID && space.Active {
			resp.Spaces = append(resp.Spaces, *space)
		}
	}
	sort.Slice(resp.Spaces, func(i, j int) bool {
		if resp.Spaces[i].ID == "default" {
			return true
		}
		if resp.Spaces[j].ID == "default" {
			return false
		}
		return resp.Spaces[i].Name < resp.Spaces[j].Name
	})
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID != accountID {
			continue
		}
		if deviceID != "" && folder.DeviceID != deviceID {
			continue
		}
		if folder.Status == syncmodel.FolderSelected && folder.SpaceID != "" {
			resp.Selected = append(resp.Selected, *folder)
		}
	}
	sort.Slice(resp.Selected, func(i, j int) bool {
		return resp.Selected[i].RootPath < resp.Selected[j].RootPath
	})
	return resp
}

func (s *Store) RequestChildren(accountID, folderID string) error {
	if folderID == "" {
		return errors.New("folder_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, folder := range s.state.ClientFolders {
		if folder.AccountID != accountID || folder.ID != folderID {
			continue
		}
		if folder.ChildrenReported {
			return nil
		}
		now := time.Now().Unix()
		folder.ChildrenRequested = true
		folder.ChildrenReported = false
		folder.ChildrenRequestedAt = now
		folder.UpdatedAt = now
		return s.saveLocked()
	}
	return errors.New("client folder not found")
}

func (s *Store) MarkChildrenReported(accountID, deviceID, rootPath string) error {
	rootPath = strings.TrimSpace(rootPath)
	if deviceID == "" {
		return errors.New("device_id is required")
	}
	if rootPath == "" {
		return errors.New("root_path is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	folder := s.state.ClientFolders[folderKey(accountID, deviceID, rootPath)]
	if folder == nil {
		return errors.New("client folder not found")
	}
	now := time.Now().Unix()
	folder.ChildrenReported = true
	folder.ChildrenReportedAt = now
	folder.UpdatedAt = now
	return s.saveLocked()
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
		now := time.Now().Unix()
		if !folder.ChildrenReported {
			folder.ChildrenRequested = true
			folder.ChildrenReported = false
			folder.ChildrenRequestedAt = now
		}
		folder.UpdatedAt = now
		return s.saveLocked()
	}
	return errors.New("client folder not found")
}

func (s *Store) SetClientStorageRoot(accountID, deviceID, storageRoot string) error {
	deviceID = strings.TrimSpace(deviceID)
	storageRoot = strings.TrimSpace(storageRoot)
	if deviceID == "" {
		return errors.New("device_id is required")
	}
	if storageRoot == "" {
		return errors.New("storage root is required")
	}
	if !isClientAbsolutePath(storageRoot) {
		return errors.New("storage root must be absolute")
	}
	cleanPath := cleanClientPath(storageRoot)
	s.mu.Lock()
	defer s.mu.Unlock()
	device := s.state.ClientDevices[deviceKey(accountID, deviceID)]
	if device == nil {
		return errors.New("client device not found")
	}
	now := time.Now().Unix()
	device.StorageRoot = cleanPath
	device.UpdatedAt = now
	for _, token := range s.state.ClientTokens {
		if token.AccountID == accountID && token.DeviceID == deviceID {
			token.Hostname = firstNonEmpty(token.Hostname, device.Hostname)
		}
	}
	return s.saveLocked()
}

func (s *Store) TouchClientDevice(accountID, deviceID, hostname, storageRoot string) {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return
	}
	hostname = strings.TrimSpace(hostname)
	storageRoot = strings.TrimSpace(storageRoot)
	if storageRoot != "" && !isClientAbsolutePath(storageRoot) {
		storageRoot = ""
	}
	storageRoot = cleanClientPath(storageRoot)
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	device := s.ensureClientDeviceLocked(accountID, deviceID, hostname, now)
	if storageRoot != "" && device.StorageRoot == "" {
		device.StorageRoot = storageRoot
		device.UpdatedAt = now
	}
	_ = s.saveLocked()
}

func isClientAbsolutePath(path string) bool {
	if filepath.IsAbs(path) || strings.HasPrefix(path, "/") {
		return true
	}
	if strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, `//`) {
		return true
	}
	if len(path) >= 3 {
		drive := path[0]
		return ((drive >= 'a' && drive <= 'z') || (drive >= 'A' && drive <= 'Z')) && path[1] == ':' && (path[2] == '\\' || path[2] == '/')
	}
	return false
}

func cleanClientPath(path string) string {
	path = strings.TrimSpace(path)
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	path = strings.ReplaceAll(path, "/", "\\")
	if strings.HasPrefix(path, "\\\\") {
		rest := strings.TrimLeft(path[2:], "\\")
		for strings.Contains(rest, "\\\\") {
			rest = strings.ReplaceAll(rest, "\\\\", "\\")
		}
		return "\\\\" + rest
	}
	for strings.Contains(path, "\\\\") {
		path = strings.ReplaceAll(path, "\\\\", "\\")
	}
	return path
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
	req.RootPath = strings.TrimSpace(req.RootPath)
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
		RootPath:    req.RootPath,
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
	req.RootPath = strings.TrimSpace(req.RootPath)
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
	if entry.Current != nil && entry.Current.State == syncmodel.EntryDeleted {
		resp := syncmodel.CommitResponse{
			Status:  "ok",
			Entry:   *entry,
			Version: *entry.Current,
		}
		s.state.Operations[opKey] = resp
		if err := s.saveLocked(); err != nil {
			return syncmodel.CommitResponse{}, err
		}
		return resp, nil
	}
	var previous syncmodel.FileVersion
	if entry.Current != nil {
		previous = *entry.Current
	}
	version := syncmodel.FileVersion{
		AccountID:   accountID,
		SpaceID:     spaceID,
		FileID:      entry.FileID,
		Path:        path,
		VersionID:   fileutil.NewID(),
		BaseVersion: req.BaseVersion,
		State:       syncmodel.EntryDeleted,
		Size:        previous.Size,
		Hash:        previous.Hash,
		Chunks:      append([]syncmodel.ChunkRef(nil), previous.Chunks...),
		ModTimeUnix: previous.ModTimeUnix,
		DeletedAt:   now,
		DeviceID:    req.DeviceID,
		RootPath:    req.RootPath,
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

func (s *Store) ListTrash(accountID string, limit int) []syncmodel.TrashEntry {
	if limit <= 0 {
		limit = 200
	}
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pruneTrashLocked(now) {
		_ = s.saveLocked()
	}
	out := make([]syncmodel.TrashEntry, 0, limit)
	for _, entry := range s.state.Files {
		if entry.AccountID != accountID || !entry.Deleted || entry.Current == nil || entry.Current.State != syncmodel.EntryDeleted {
			continue
		}
		trash := trashEntryFromVersion(*entry.Current)
		if trash.ExpiresAt <= now {
			continue
		}
		out = append(out, trash)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].DeletedAt > out[j].DeletedAt
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (s *Store) RestoreTrash(accountID string, req syncmodel.RestoreRequest, actor string) (syncmodel.CommitResponse, error) {
	spaceID := strings.TrimSpace(req.SpaceID)
	if spaceID == "" {
		spaceID = "default"
	}
	if _, ok := s.GetSpace(accountID, spaceID); !ok {
		return syncmodel.CommitResponse{}, errors.New("sync space not found")
	}
	path, err := fileutil.CleanRel(req.Path)
	if err != nil {
		return syncmodel.CommitResponse{}, err
	}
	fileID := strings.TrimSpace(req.FileID)
	if fileID == "" {
		return syncmodel.CommitResponse{}, errors.New("file_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	pruned := s.pruneTrashLocked(now)
	entry := s.state.Files[fileKey(accountID, spaceID, path)]
	if entry == nil || entry.FileID != fileID || !entry.Deleted || entry.Current == nil || entry.Current.State != syncmodel.EntryDeleted {
		if pruned {
			_ = s.saveLocked()
		}
		return syncmodel.CommitResponse{}, errors.New("trash entry not found")
	}
	if entry.Current.DeletedAt+syncmodel.TrashRetentionSeconds <= now {
		if pruned {
			_ = s.saveLocked()
		}
		return syncmodel.CommitResponse{}, errors.New("trash entry has expired")
	}
	versions := s.state.Versions[fileID]
	var source *syncmodel.FileVersion
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].State == syncmodel.EntryFile {
			copy := versions[i]
			source = &copy
			break
		}
	}
	if source == nil && entry.Current.Hash != "" {
		copy := *entry.Current
		copy.State = syncmodel.EntryFile
		source = &copy
	}
	if source == nil {
		if pruned {
			_ = s.saveLocked()
		}
		return syncmodel.CommitResponse{}, errors.New("restorable version not found")
	}
	deviceID := "admin"
	if strings.TrimSpace(actor) != "" {
		deviceID = "admin-" + safeDeviceName(actor)
	}
	version := syncmodel.FileVersion{
		AccountID:   accountID,
		SpaceID:     spaceID,
		FileID:      entry.FileID,
		Path:        path,
		VersionID:   fileutil.NewID(),
		BaseVersion: entry.Current.VersionID,
		State:       syncmodel.EntryFile,
		Size:        source.Size,
		Hash:        source.Hash,
		Chunks:      append([]syncmodel.ChunkRef(nil), source.Chunks...),
		ModTimeUnix: now,
		DeviceID:    deviceID,
		RootPath:    "cloud-trash",
		CreatedAt:   now,
	}
	entry.Current = &version
	entry.Deleted = false
	entry.LatestVersion = version.VersionID
	entry.UpdatedAt = now
	s.state.Versions[entry.FileID] = append(s.state.Versions[entry.FileID], version)
	s.appendEventLocked(version)
	resp := syncmodel.CommitResponse{
		Status:  "ok",
		Entry:   *entry,
		Version: version,
	}
	if err := s.saveLocked(); err != nil {
		return syncmodel.CommitResponse{}, err
	}
	return resp, nil
}

func (s *Store) AddSyncRecord(accountID string, req syncmodel.SyncRecordRequest) (syncmodel.SyncRecord, error) {
	action := strings.TrimSpace(req.Action)
	status := strings.TrimSpace(req.Status)
	if action == "" {
		return syncmodel.SyncRecord{}, errors.New("action is required")
	}
	if status == "" {
		return syncmodel.SyncRecord{}, errors.New("status is required")
	}
	if status != syncmodel.SyncRecordStatusSuccess && status != syncmodel.SyncRecordStatusFailed {
		return syncmodel.SyncRecord{}, errors.New("invalid sync record status")
	}
	now := time.Now().Unix()
	record := syncmodel.SyncRecord{
		ID:             fileutil.NewID(),
		AccountID:      accountID,
		SpaceID:        strings.TrimSpace(req.SpaceID),
		DeviceID:       strings.TrimSpace(req.DeviceID),
		RootPath:       strings.TrimSpace(req.RootPath),
		Path:           strings.TrimSpace(req.Path),
		Action:         action,
		Status:         status,
		Error:          strings.TrimSpace(req.Error),
		DurationMillis: req.DurationMillis,
		CreatedAt:      now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.SyncRecords = append(s.state.SyncRecords, record)
	s.trimSyncRecordsLocked(accountID)
	if err := s.saveLocked(); err != nil {
		return syncmodel.SyncRecord{}, err
	}
	return record, nil
}

func (s *Store) ListSyncRecords(accountID string, limit int) []syncmodel.SyncRecord {
	if limit <= 0 {
		limit = 200
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]syncmodel.SyncRecord, 0, limit)
	for i := len(s.state.SyncRecords) - 1; i >= 0 && len(out) < limit; i-- {
		record := s.state.SyncRecords[i]
		if record.AccountID == accountID {
			out = append(out, record)
		}
	}
	return out
}

func (s *Store) trimSyncRecordsLocked(accountID string) {
	count := 0
	for i := len(s.state.SyncRecords) - 1; i >= 0; i-- {
		if s.state.SyncRecords[i].AccountID != accountID {
			continue
		}
		count++
		if count <= syncmodel.MaxSyncRecordsPerAccount {
			continue
		}
		s.state.SyncRecords = append(s.state.SyncRecords[:i], s.state.SyncRecords[i+1:]...)
	}
}

func (s *Store) ensureClientDeviceLocked(accountID, deviceID, hostname string, now int64) *syncmodel.ClientDevice {
	key := deviceKey(accountID, deviceID)
	device := s.state.ClientDevices[key]
	if device == nil {
		device = &syncmodel.ClientDevice{
			AccountID:  accountID,
			DeviceID:   deviceID,
			Hostname:   hostname,
			CreatedAt:  now,
			UpdatedAt:  now,
			LastSeenAt: now,
		}
		s.state.ClientDevices[key] = device
		return device
	}
	if hostname != "" {
		device.Hostname = hostname
	}
	device.LastSeenAt = now
	device.UpdatedAt = now
	return device
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
		RootPath:  version.RootPath,
		CreatedAt: version.CreatedAt,
		Version:   version,
	}
	s.state.Events = append(s.state.Events, event)
	if len(s.state.Events) > maxEvents {
		s.state.Events = s.state.Events[len(s.state.Events)-maxEvents:]
	}
}

func (s *Store) pruneTrashLocked(now int64) bool {
	changed := false
	for key, entry := range s.state.Files {
		if entry == nil || !entry.Deleted || entry.Current == nil || entry.Current.State != syncmodel.EntryDeleted {
			continue
		}
		if entry.Current.DeletedAt <= 0 || entry.Current.DeletedAt+syncmodel.TrashRetentionSeconds > now {
			continue
		}
		delete(s.state.Files, key)
		changed = true
	}
	return changed
}

func trashEntryFromVersion(version syncmodel.FileVersion) syncmodel.TrashEntry {
	return syncmodel.TrashEntry{
		AccountID: version.AccountID,
		SpaceID:   version.SpaceID,
		FileID:    version.FileID,
		Path:      version.Path,
		VersionID: version.VersionID,
		Size:      version.Size,
		Hash:      version.Hash,
		DeletedAt: version.DeletedAt,
		ExpiresAt: version.DeletedAt + syncmodel.TrashRetentionSeconds,
		DeviceID:  version.DeviceID,
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

func safeDeviceName(v string) string {
	v = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, v)
	v = strings.Trim(v, "-")
	if v == "" {
		return "admin"
	}
	return v
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

func deviceKey(accountID, deviceID string) string {
	return accountID + "\x00" + strings.TrimSpace(deviceID)
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
