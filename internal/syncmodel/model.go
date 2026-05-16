package syncmodel

const (
	DefaultChunkSize = 4 * 1024 * 1024

	EntryFile    = "file"
	EntryDeleted = "deleted"
)

type ChunkRef struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"`
	Size  int64  `json:"size"`
}

type FileVersion struct {
	FileID      string     `json:"file_id"`
	Path        string     `json:"path"`
	VersionID   string     `json:"version_id"`
	BaseVersion string     `json:"base_version,omitempty"`
	State       string     `json:"state"`
	Size        int64      `json:"size"`
	Hash        string     `json:"hash,omitempty"`
	Chunks      []ChunkRef `json:"chunks,omitempty"`
	ModTimeUnix int64      `json:"mod_time_unix,omitempty"`
	DeletedAt   int64      `json:"deleted_at,omitempty"`
	DeviceID    string     `json:"device_id"`
	CreatedAt   int64      `json:"created_at"`
}

type FileEntry struct {
	FileID        string       `json:"file_id"`
	Path          string       `json:"path"`
	Current       *FileVersion `json:"current,omitempty"`
	Deleted       bool         `json:"deleted"`
	LatestVersion string       `json:"latest_version,omitempty"`
	UpdatedAt     int64        `json:"updated_at"`
}

type LocalFileState struct {
	Path        string     `json:"path"`
	FileID      string     `json:"file_id"`
	VersionID   string     `json:"version_id"`
	State       string     `json:"state"`
	Size        int64      `json:"size"`
	Hash        string     `json:"hash,omitempty"`
	Chunks      []ChunkRef `json:"chunks,omitempty"`
	ModTimeUnix int64      `json:"mod_time_unix,omitempty"`
	UpdatedAt   int64      `json:"updated_at"`
}

type ServerState struct {
	Files        map[string]*FileEntry     `json:"files"`
	Versions     map[string][]FileVersion  `json:"versions"`
	ChunkRefs    map[string]int            `json:"chunk_refs"`
	DeviceSeq    map[string]int64          `json:"device_seq"`
	Operations   map[string]CommitResponse `json:"operations"`
	LastEventSeq int64                     `json:"last_event_seq"`
	Events       []Event                   `json:"events"`
}

type Event struct {
	Seq       int64       `json:"seq"`
	Path      string      `json:"path"`
	FileID    string      `json:"file_id"`
	VersionID string      `json:"version_id"`
	State     string      `json:"state"`
	DeviceID  string      `json:"device_id"`
	CreatedAt int64       `json:"created_at"`
	Version   FileVersion `json:"version"`
}

type Manifest struct {
	FileID      string     `json:"file_id,omitempty"`
	Path        string     `json:"path"`
	BaseVersion string     `json:"base_version,omitempty"`
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
	Path        string `json:"path"`
	FileID      string `json:"file_id,omitempty"`
	BaseVersion string `json:"base_version,omitempty"`
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
