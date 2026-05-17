package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"xcloud/internal/syncmodel"
)

const (
	apiRetryMaxAttempts = 5
)

var (
	apiRetryInitialBackoff = 500 * time.Millisecond
	apiRetryMaxBackoff     = 8 * time.Second
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

func (a *API) ReportSyncRecord(req syncmodel.SyncRecordRequest) error {
	return a.doJSON(http.MethodPost, "/v1/sync-records", req, nil)
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
	return a.doWithRetry(func() (*http.Request, error) {
		req, err := http.NewRequest(http.MethodPut, a.base+"/v1/chunks/"+url.PathEscape(hash), bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		a.auth(req)
		req.Header.Set("Content-Type", "application/octet-stream")
		return req, nil
	}, func(resp *http.Response) error {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return statusResponseError{err: fmt.Errorf("upload chunk failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))}
		}
		return nil
	})
}

func (a *API) DownloadChunk(hash string) ([]byte, error) {
	var data []byte
	err := a.doWithRetry(func() (*http.Request, error) {
		req, err := http.NewRequest(http.MethodGet, a.base+"/v1/chunks/"+url.PathEscape(hash), nil)
		if err != nil {
			return nil, err
		}
		a.auth(req)
		return req, nil
	}, func(resp *http.Response) error {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return statusResponseError{err: fmt.Errorf("download chunk failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))}
		}
		var err error
		data, err = io.ReadAll(resp.Body)
		return err
	})
	return data, err
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
	var payload []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		payload = b
	}
	return a.doWithRetry(func() (*http.Request, error) {
		var body io.Reader
		if payload != nil {
			body = bytes.NewReader(payload)
		}
		req, err := http.NewRequest(method, a.base+path, body)
		if err != nil {
			return nil, err
		}
		a.auth(req)
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		return req, nil
	}, func(resp *http.Response) error {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return statusResponseError{err: fmt.Errorf("%s %s failed: %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))}
		}
		if out == nil {
			return nil
		}
		return json.NewDecoder(resp.Body).Decode(out)
	})
}

func (a *API) doWithRetry(newRequest func() (*http.Request, error), handleResponse func(*http.Response) error) error {
	var lastErr error
	for attempt := 1; attempt <= apiRetryMaxAttempts; attempt++ {
		req, err := newRequest()
		if err != nil {
			return err
		}
		resp, err := a.client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < apiRetryMaxAttempts && retryableHTTPError(err) {
				sleepRetryBackoff(attempt, 0)
				continue
			}
			return err
		}
		if shouldRetryStatus(resp.StatusCode) && attempt < apiRetryMaxAttempts {
			delay := retryAfterDelay(resp)
			drainAndClose(resp.Body)
			lastErr = fmt.Errorf("%s %s failed: %s", req.Method, req.URL.RequestURI(), resp.Status)
			sleepRetryBackoff(attempt, delay)
			continue
		}
		err = handleResponse(resp)
		closeErr := resp.Body.Close()
		if err != nil {
			var statusErr statusResponseError
			if errors.As(err, &statusErr) {
				return err
			}
			lastErr = err
			if attempt < apiRetryMaxAttempts && retryableHTTPError(err) {
				sleepRetryBackoff(attempt, 0)
				continue
			}
			return err
		}
		if closeErr != nil {
			lastErr = closeErr
			if attempt < apiRetryMaxAttempts && retryableHTTPError(closeErr) {
				sleepRetryBackoff(attempt, 0)
				continue
			}
			return closeErr
		}
		return nil
	}
	return lastErr
}

type statusResponseError struct {
	err error
}

func (e statusResponseError) Error() string {
	return e.err.Error()
}

func (e statusResponseError) Unwrap() error {
	return e.err
}

func retryableHTTPError(err error) bool {
	return err != nil
}

func shouldRetryStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func retryAfterDelay(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		delay := time.Until(when)
		if delay > 0 {
			return delay
		}
	}
	return 0
}

func sleepRetryBackoff(attempt int, serverDelay time.Duration) {
	delay := retryBackoff(attempt)
	if serverDelay > delay {
		delay = serverDelay
	}
	if delay > apiRetryMaxBackoff {
		delay = apiRetryMaxBackoff
	}
	if delay <= 0 {
		return
	}
	time.Sleep(delay)
}

func retryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	delay := apiRetryInitialBackoff
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= apiRetryMaxBackoff {
			return apiRetryMaxBackoff
		}
	}
	if delay > apiRetryMaxBackoff {
		return apiRetryMaxBackoff
	}
	return delay
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4096))
	_ = body.Close()
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
