package syncmodel

const (
	DefaultChunkSize           = 4 * 1024 * 1024
	DefaultSyncIntervalSeconds = 10
	DefaultSyncDebounceMillis  = 800
	MaxSyncRecordsPerAccount   = 5000
	TrashRetentionSeconds      = 10 * 24 * 60 * 60

	EntryFile    = "file"
	EntryDir     = "dir"
	EntryDeleted = "deleted"

	FolderPending  = "pending"
	FolderSelected = "selected"
	FolderDisabled = "disabled"

	SyncActionUpload        = "upload"
	SyncActionDownload      = "download"
	SyncActionDelete        = "delete"
	SyncActionConflict      = "conflict"
	SyncActionSkip          = "skip"
	SyncActionScan          = "scan"
	SyncActionWatch         = "watch"
	SyncRecordStatusSuccess = "success"
	SyncRecordStatusFailed  = "failed"
)

type Account struct {
	ID            string       `json:"id"`
	Username      string       `json:"username"`
	DisplayName   string       `json:"display_name,omitempty"`
	Email         string       `json:"email,omitempty"`
	PasswordHash  string       `json:"password_hash"`
	SyncTokenHash string       `json:"sync_token_hash"`
	IsAdmin       bool         `json:"is_admin"`
	Disabled      bool         `json:"disabled"`
	SyncEnabled   bool         `json:"sync_enabled,omitempty"`
	SyncSettings  SyncSettings `json:"sync_settings"`
	CreatedAt     int64        `json:"created_at"`
	UpdatedAt     int64        `json:"updated_at"`
}

type SyncSettings struct {
	RealtimeEnabled bool `json:"realtime_enabled"`
	DebounceMillis  int  `json:"debounce_millis"`
	IntervalSeconds int  `json:"interval_seconds"`
}

func DefaultSyncSettings() SyncSettings {
	return SyncSettings{
		RealtimeEnabled: true,
		DebounceMillis:  DefaultSyncDebounceMillis,
		IntervalSeconds: DefaultSyncIntervalSeconds,
	}
}

func NormalizeSyncSettings(settings SyncSettings) SyncSettings {
	if settings.DebounceMillis <= 0 {
		settings.DebounceMillis = DefaultSyncDebounceMillis
	}
	if settings.IntervalSeconds <= 0 {
		settings.IntervalSeconds = DefaultSyncIntervalSeconds
	}
	if settings.IntervalSeconds < 1 {
		settings.IntervalSeconds = 1
	}
	if settings.DebounceMillis < 100 {
		settings.DebounceMillis = 100
	}
	return settings
}

type AccountProfile struct {
	ID          string `json:"id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name,omitempty"`
	Email       string `json:"email,omitempty"`
	IsAdmin     bool   `json:"is_admin"`
}

type ClientToken struct {
	ID         string `json:"id"`
	AccountID  string `json:"account_id"`
	DeviceID   string `json:"device_id,omitempty"`
	Hostname   string `json:"hostname,omitempty"`
	TokenHash  string `json:"token_hash"`
	Disabled   bool   `json:"disabled,omitempty"`
	CreatedAt  int64  `json:"created_at"`
	LastUsedAt int64  `json:"last_used_at,omitempty"`
}

type ClientDevice struct {
	AccountID   string `json:"account_id"`
	DeviceID    string `json:"device_id"`
	Hostname    string `json:"hostname,omitempty"`
	StorageRoot string `json:"storage_root,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
	LastSeenAt  int64  `json:"last_seen_at,omitempty"`
}

type SyncSpace struct {
	ID          string `json:"id"`
	AccountID   string `json:"account_id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Active      bool   `json:"active"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

type ClientFolder struct {
	ID                  string `json:"id"`
	AccountID           string `json:"account_id"`
	DeviceID            string `json:"device_id"`
	Hostname            string `json:"hostname,omitempty"`
	RootPath            string `json:"root_path"`
	ParentPath          string `json:"parent_path,omitempty"`
	Depth               int    `json:"depth"`
	SuggestedSpaceID    string `json:"suggested_space_id,omitempty"`
	SpaceID             string `json:"space_id,omitempty"`
	Status              string `json:"status"`
	ChildrenRequested   bool   `json:"children_requested,omitempty"`
	ChildrenReported    bool   `json:"children_reported,omitempty"`
	ChildrenRequestedAt int64  `json:"children_requested_at,omitempty"`
	ChildrenReportedAt  int64  `json:"children_reported_at,omitempty"`
	LastSeenAt          int64  `json:"last_seen_at"`
	CreatedAt           int64  `json:"created_at"`
	UpdatedAt           int64  `json:"updated_at"`
}

type ChunkRef struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"`
	Size  int64  `json:"size"`
}

type FileVersion struct {
	AccountID    string     `json:"account_id,omitempty"`
	SpaceID      string     `json:"space_id,omitempty"`
	FileID       string     `json:"file_id"`
	Path         string     `json:"path"`
	VersionID    string     `json:"version_id"`
	BaseVersion  string     `json:"base_version,omitempty"`
	State        string     `json:"state"`
	DeletedState string     `json:"deleted_state,omitempty"`
	Size         int64      `json:"size"`
	Hash         string     `json:"hash,omitempty"`
	Chunks       []ChunkRef `json:"chunks,omitempty"`
	ModTimeUnix  int64      `json:"mod_time_unix,omitempty"`
	DeletedAt    int64      `json:"deleted_at,omitempty"`
	DeviceID     string     `json:"device_id"`
	RootPath     string     `json:"root_path,omitempty"`
	CreatedAt    int64      `json:"created_at"`
}

type FileEntry struct {
	AccountID     string       `json:"account_id,omitempty"`
	SpaceID       string       `json:"space_id,omitempty"`
	FileID        string       `json:"file_id"`
	Path          string       `json:"path"`
	Current       *FileVersion `json:"current,omitempty"`
	Deleted       bool         `json:"deleted"`
	LatestVersion string       `json:"latest_version,omitempty"`
	UpdatedAt     int64        `json:"updated_at"`
}

type LocalFileState struct {
	Path         string     `json:"path"`
	FileID       string     `json:"file_id"`
	VersionID    string     `json:"version_id"`
	State        string     `json:"state"`
	DeletedState string     `json:"deleted_state,omitempty"`
	Size         int64      `json:"size"`
	Hash         string     `json:"hash,omitempty"`
	Chunks       []ChunkRef `json:"chunks,omitempty"`
	ModTimeUnix  int64      `json:"mod_time_unix,omitempty"`
	UpdatedAt    int64      `json:"updated_at"`
}

type ServerState struct {
	Accounts      map[string]*Account       `json:"accounts"`
	ClientTokens  map[string]*ClientToken   `json:"client_tokens,omitempty"`
	ClientDevices map[string]*ClientDevice  `json:"client_devices,omitempty"`
	Spaces        map[string]*SyncSpace     `json:"spaces"`
	ClientFolders map[string]*ClientFolder  `json:"client_folders"`
	Files         map[string]*FileEntry     `json:"files"`
	Versions      map[string][]FileVersion  `json:"versions"`
	ChunkRefs     map[string]int            `json:"chunk_refs"`
	AccountChunks map[string]bool           `json:"account_chunks"`
	DeviceSeq     map[string]int64          `json:"device_seq"`
	Operations    map[string]CommitResponse `json:"operations"`
	LastEventSeq  int64                     `json:"last_event_seq"`
	Events        []Event                   `json:"events"`
	SyncRecords   []SyncRecord              `json:"sync_records,omitempty"`
}

type SyncRecord struct {
	ID             string `json:"id"`
	AccountID      string `json:"account_id,omitempty"`
	SpaceID        string `json:"space_id,omitempty"`
	DeviceID       string `json:"device_id,omitempty"`
	RootPath       string `json:"root_path,omitempty"`
	Path           string `json:"path,omitempty"`
	Action         string `json:"action"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
	DurationMillis int64  `json:"duration_millis,omitempty"`
	CreatedAt      int64  `json:"created_at"`
}

type TrashEntry struct {
	AccountID string `json:"account_id,omitempty"`
	SpaceID   string `json:"space_id,omitempty"`
	FileID    string `json:"file_id"`
	Path      string `json:"path"`
	VersionID string `json:"version_id"`
	Size      int64  `json:"size"`
	Hash      string `json:"hash,omitempty"`
	DeletedAt int64  `json:"deleted_at"`
	ExpiresAt int64  `json:"expires_at"`
	DeviceID  string `json:"device_id,omitempty"`
}

type Event struct {
	AccountID string      `json:"account_id,omitempty"`
	SpaceID   string      `json:"space_id,omitempty"`
	Seq       int64       `json:"seq"`
	Path      string      `json:"path"`
	FileID    string      `json:"file_id"`
	VersionID string      `json:"version_id"`
	State     string      `json:"state"`
	DeviceID  string      `json:"device_id"`
	RootPath  string      `json:"root_path,omitempty"`
	CreatedAt int64       `json:"created_at"`
	Version   FileVersion `json:"version"`
}

type FolderReportRequest struct {
	DeviceID         string `json:"device_id"`
	Hostname         string `json:"hostname,omitempty"`
	RootPath         string `json:"root_path"`
	StorageRoot      string `json:"storage_root,omitempty"`
	ParentPath       string `json:"parent_path,omitempty"`
	Depth            int    `json:"depth,omitempty"`
	SuggestedSpaceID string `json:"suggested_space_id,omitempty"`
}

type FolderDiscoveryRequest struct {
	RootPath string `json:"root_path"`
	Depth    int    `json:"depth"`
}

type FolderReportResponse struct {
	Folder   ClientFolder `json:"folder"`
	Space    *SyncSpace   `json:"space,omitempty"`
	Selected bool         `json:"selected"`
}

type FolderStatusResponse struct {
	Requests    []FolderDiscoveryRequest `json:"requests"`
	Selected    []ClientFolder           `json:"selected"`
	Settings    SyncSettings             `json:"settings"`
	StorageRoot string                   `json:"storage_root,omitempty"`
}

type FolderChildrenCompleteRequest struct {
	DeviceID string `json:"device_id"`
	RootPath string `json:"root_path"`
}

type ClientLoginRequest struct {
	Identifier  string `json:"identifier"`
	Password    string `json:"password"`
	DeviceID    string `json:"device_id,omitempty"`
	Hostname    string `json:"hostname,omitempty"`
	StorageRoot string `json:"storage_root,omitempty"`
}

type ClientLoginResponse struct {
	Account     AccountProfile `json:"account"`
	Token       string         `json:"token"`
	SpaceID     string         `json:"space_id"`
	SyncEnabled bool           `json:"sync_enabled"`
	Settings    SyncSettings   `json:"settings"`
	StorageRoot string         `json:"storage_root,omitempty"`
}

type ClientStatusResponse struct {
	Account     AccountProfile `json:"account"`
	SpaceID     string         `json:"space_id"`
	SyncEnabled bool           `json:"sync_enabled"`
	Settings    SyncSettings   `json:"settings"`
	StorageRoot string         `json:"storage_root,omitempty"`
	Spaces      []SyncSpace    `json:"spaces,omitempty"`
	Selected    []ClientFolder `json:"selected,omitempty"`
}

type ClientSyncToggleRequest struct {
	Enabled bool `json:"enabled"`
}

type SyncSettingsUpdateRequest struct {
	AccountID       string `json:"account_id,omitempty"`
	RealtimeEnabled bool   `json:"realtime_enabled"`
	DebounceMillis  int    `json:"debounce_millis"`
	IntervalSeconds int    `json:"interval_seconds"`
}

type ClientStorageRootRequest struct {
	AccountID   string `json:"account_id,omitempty"`
	DeviceID    string `json:"device_id"`
	StorageRoot string `json:"storage_root"`
}

type SyncRecordRequest struct {
	SpaceID        string `json:"space_id,omitempty"`
	DeviceID       string `json:"device_id,omitempty"`
	RootPath       string `json:"root_path,omitempty"`
	Path           string `json:"path,omitempty"`
	Action         string `json:"action"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
	DurationMillis int64  `json:"duration_millis,omitempty"`
}

type Manifest struct {
	FileID      string     `json:"file_id,omitempty"`
	Path        string     `json:"path"`
	BaseVersion string     `json:"base_version,omitempty"`
	State       string     `json:"state,omitempty"`
	Size        int64      `json:"size"`
	Hash        string     `json:"hash"`
	Chunks      []ChunkRef `json:"chunks"`
	ModTimeUnix int64      `json:"mod_time_unix"`
}

type CheckChunksRequest struct {
	Chunks []string `json:"chunks"`
}

type CheckChunksResponse struct {
	Missing []string `json:"missing"`
}

type CommitRequest struct {
	OperationID string   `json:"operation_id"`
	DeviceID    string   `json:"device_id"`
	RootPath    string   `json:"root_path,omitempty"`
	Manifest    Manifest `json:"manifest"`
}

type CommitResponse struct {
	Status         string       `json:"status"`
	Entry          FileEntry    `json:"entry"`
	Version        FileVersion  `json:"version"`
	Conflict       bool         `json:"conflict"`
	ConflictPath   string       `json:"conflict_path,omitempty"`
	CurrentVersion *FileVersion `json:"current_version,omitempty"`
}

type DeleteRequest struct {
	OperationID string `json:"operation_id"`
	DeviceID    string `json:"device_id"`
	RootPath    string `json:"root_path,omitempty"`
	Path        string `json:"path"`
	FileID      string `json:"file_id,omitempty"`
	BaseVersion string `json:"base_version,omitempty"`
}

type RestoreRequest struct {
	AccountID string `json:"account_id,omitempty"`
	SpaceID   string `json:"space_id"`
	FileID    string `json:"file_id"`
	Path      string `json:"path"`
}

type ListResponse struct {
	Files []FileEntry `json:"files"`
}

type EventsResponse struct {
	Events []Event `json:"events"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type SpaceSummary struct {
	Space     SyncSpace `json:"space"`
	FileCount int       `json:"file_count"`
	Deleted   int       `json:"deleted"`
	Folders   int       `json:"folders"`
	Trash     int       `json:"trash"`
}
