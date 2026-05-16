package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"xcloud/internal/syncmodel"
)

type API struct {
	base     string
	token    string
	space    string
	deviceID string
	rootPath string
	client   *http.Client
}

func NewAPI(base, token, space, deviceID, rootPath string) *API {
	return &API{
		base:     strings.TrimRight(base, "/"),
		token:    token,
		space:    space,
		deviceID: deviceID,
		rootPath: rootPath,
		client: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

func (a *API) SetSyncContext(space, deviceID, rootPath string) {
	a.space = space
	a.deviceID = deviceID
	a.rootPath = rootPath
}

func (a *API) ReportFolder(req syncmodel.FolderReportRequest) (syncmodel.FolderReportResponse, error) {
	var resp syncmodel.FolderReportResponse
	err := a.doJSON(http.MethodPost, "/v1/folders/report", req, &resp)
	return resp, err
}

func (a *API) FolderStatus() (syncmodel.FolderStatusResponse, error) {
	var resp syncmodel.FolderStatusResponse
	err := a.doJSON(http.MethodGet, "/v1/folders/status", nil, &resp)
	return resp, err
}

func (a *API) ChildrenComplete(req syncmodel.FolderChildrenCompleteRequest) error {
	return a.doJSON(http.MethodPost, "/v1/folders/children-complete", req, nil)
}

func (a *API) ClientLogin(req syncmodel.ClientLoginRequest) (syncmodel.ClientLoginResponse, error) {
	var resp syncmodel.ClientLoginResponse
	err := a.doJSON(http.MethodPost, "/v1/client/login", req, &resp)
	return resp, err
}

func (a *API) ClientStatus() (syncmodel.ClientStatusResponse, error) {
	var resp syncmodel.ClientStatusResponse
	err := a.doJSON(http.MethodGet, "/v1/client/status", nil, &resp)
	return resp, err
}

func (a *API) SetClientSyncEnabled(enabled bool) (syncmodel.ClientStatusResponse, error) {
	var resp syncmodel.ClientStatusResponse
	err := a.doJSON(http.MethodPost, "/v1/client/sync", syncmodel.ClientSyncToggleRequest{Enabled: enabled}, &resp)
	return resp, err
}

func (a *API) ListFiles() ([]syncmodel.FileEntry, error) {
	var resp syncmodel.ListResponse
	if err := a.doJSON(http.MethodGet, "/v1/files", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Files, nil
}

func (a *API) Events(after int64) ([]syncmodel.Event, error) {
	var resp syncmodel.EventsResponse
	if err := a.doJSON(http.MethodGet, fmt.Sprintf("/v1/events?after=%d", after), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Events, nil
}

func (a *API) CheckChunks(chunks []string) ([]string, error) {
	var resp syncmodel.CheckChunksResponse
	err := a.doJSON(http.MethodPost, "/v1/chunks/check", syncmodel.CheckChunksRequest{Chunks: chunks}, &resp)
	return resp.Missing, err
}

func (a *API) UploadChunk(hash string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, a.base+"/v1/chunks/"+url.PathEscape(hash), bytes.NewReader(data))
	if err != nil {
		return err
	}
	a.auth(req)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("upload chunk failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (a *API) DownloadChunk(hash string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, a.base+"/v1/chunks/"+url.PathEscape(hash), nil)
	if err != nil {
		return nil, err
	}
	a.auth(req)
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("download chunk failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

func (a *API) Commit(req syncmodel.CommitRequest) (syncmodel.CommitResponse, error) {
	var resp syncmodel.CommitResponse
	err := a.doJSON(http.MethodPost, "/v1/commit", req, &resp)
	return resp, err
}

func (a *API) Delete(req syncmodel.DeleteRequest) (syncmodel.CommitResponse, error) {
	var resp syncmodel.CommitResponse
	err := a.doJSON(http.MethodPost, "/v1/delete", req, &resp)
	return resp, err
}

func (a *API) doJSON(method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, a.base+path, body)
	if err != nil {
		return err
	}
	a.auth(req)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s failed: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (a *API) auth(req *http.Request) {
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	if a.space != "" {
		req.Header.Set("X-XCloud-Space", a.space)
	}
	if a.deviceID != "" {
		req.Header.Set("X-XCloud-Device", a.deviceID)
	}
	if a.rootPath != "" {
		req.Header.Set("X-XCloud-Root", a.rootPath)
	}
}
