package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"xcloud/internal/fileutil"
	"xcloud/internal/syncmodel"
)

const sessionCookie = "xcloud_session"

type Server struct {
	store         *Store
	log           *slog.Logger
	sessions      map[string]string
	runtimeConfig RuntimeConfig
	mu            sync.Mutex
}

type syncContext struct {
	account  syncmodel.Account
	spaceID  string
	deviceID string
	rootPath string
}

func New(store *Store, _ string, log *slog.Logger) *Server {
	return NewWithRuntimeConfig(store, "", log, DefaultRuntimeConfig(""))
}

func NewWithRuntimeConfig(store *Store, _ string, log *slog.Logger, runtimeConfig RuntimeConfig) *Server {
	if log == nil {
		log = slog.Default()
	}
	runtimeConfig.Normalize()
	return &Server{
		store:         store,
		log:           log,
		sessions:      map[string]string{},
		runtimeConfig: runtimeConfig,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.redirectAdmin)
	mux.HandleFunc("GET /healthz", s.healthz)

	mux.HandleFunc("GET /admin", s.admin)
	mux.HandleFunc("POST /admin/login", s.adminLogin)
	mux.HandleFunc("POST /admin/register", s.adminRegister)
	mux.HandleFunc("POST /admin/logout", s.adminLogout)
	mux.HandleFunc("POST /admin/accounts/create", s.requireAdmin(s.adminCreateAccount))
	mux.HandleFunc("POST /admin/accounts/reset-token", s.requireLogin(s.adminResetToken))
	mux.HandleFunc("POST /admin/accounts/toggle", s.requireAdmin(s.adminToggleAccount))
	mux.HandleFunc("POST /admin/accounts/change-password", s.requireLogin(s.adminChangePassword))
	mux.HandleFunc("POST /admin/spaces/create", s.requireLogin(s.adminCreateSpace))
	mux.HandleFunc("POST /admin/spaces/toggle", s.requireLogin(s.adminToggleSpace))
	mux.HandleFunc("POST /admin/clients/storage-root", s.requireLogin(s.adminSetClientStorageRoot))
	mux.HandleFunc("POST /admin/trash/restore", s.requireLogin(s.adminRestoreTrash))
	mux.HandleFunc("POST /admin/sync-settings/update", s.requireLogin(s.adminUpdateSyncSettings))
	mux.HandleFunc("POST /admin/server-config/update", s.requireAdmin(s.adminUpdateServerConfig))

	mux.HandleFunc("POST /v1/client/login", s.clientLogin)
	mux.HandleFunc("GET /v1/client/status", s.clientAuth(s.clientStatus))
	mux.HandleFunc("POST /v1/client/sync", s.clientAuth(s.clientSyncToggle))
	mux.HandleFunc("POST /v1/sync-records", s.clientAuth(s.createSyncRecord))
	mux.HandleFunc("POST /v1/folders/report", s.syncAuth(s.reportFolder))
	mux.HandleFunc("GET /v1/folders/status", s.syncAuth(s.folderStatus))
	mux.HandleFunc("POST /v1/folders/children-complete", s.syncAuth(s.childrenComplete))
	mux.HandleFunc("GET /v1/files", s.syncAuth(s.listFiles))
	mux.HandleFunc("GET /v1/events", s.syncAuth(s.events))
	mux.HandleFunc("POST /v1/chunks/check", s.syncAuth(s.checkChunks))
	mux.HandleFunc("PUT /v1/chunks/", s.syncAuth(s.putChunk))
	mux.HandleFunc("GET /v1/chunks/", s.syncAuth(s.getChunk))
	mux.HandleFunc("POST /v1/commit", s.syncAuth(s.commit))
	mux.HandleFunc("POST /v1/delete", s.syncAuth(s.delete))
	return mux
}

func (s *Server) redirectAdmin(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) syncAuth(next func(http.ResponseWriter, *http.Request, syncContext)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		account, ok := s.store.AuthenticateSyncToken(token)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, syncmodel.ErrorResponse{Error: "unauthorized"})
			return
		}
		if !account.SyncEnabled {
			writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "account sync is disabled"})
			return
		}
		spaceID := strings.TrimSpace(r.Header.Get("X-XCloud-Space"))
		if spaceID == "" {
			spaceID = strings.TrimSpace(r.URL.Query().Get("space"))
		}
		if spaceID == "" {
			spaceID = "default"
		}
		if strings.Contains(spaceID, "/") || strings.Contains(spaceID, "\\") || strings.Contains(spaceID, "..") {
			writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: "invalid sync space"})
			return
		}
		deviceID := strings.TrimSpace(r.Header.Get("X-XCloud-Device"))
		rootPath := strings.TrimSpace(r.Header.Get("X-XCloud-Root"))
		if r.URL.Path != "/v1/folders/report" && r.URL.Path != "/v1/folders/status" && r.URL.Path != "/v1/folders/children-complete" {
			if deviceID == "" || rootPath == "" {
				writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "client device and root are required"})
				return
			}
			if _, ok := s.store.GetSpace(account.ID, spaceID); !ok {
				writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "sync space not found"})
				return
			}
			s.store.TouchClientDevice(account.ID, deviceID, "", "")
		}
		next(w, r, syncContext{account: *account, spaceID: spaceID, deviceID: deviceID, rootPath: rootPath})
	}
}

func (s *Server) clientAuth(next func(http.ResponseWriter, *http.Request, syncmodel.Account)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		account, ok := s.store.AccountForSyncToken(token)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, syncmodel.ErrorResponse{Error: "unauthorized"})
			return
		}
		next(w, r, *account)
	}
}

func (s *Server) reportFolder(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	var req syncmodel.FolderReportRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.SuggestedSpaceID) == "" {
		req.SuggestedSpaceID = ctx.spaceID
	}
	resp, err := s.store.ReportFolder(ctx.account.ID, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) folderStatus(w http.ResponseWriter, _ *http.Request, ctx syncContext) {
	writeJSON(w, http.StatusOK, s.store.FolderStatus(ctx.account.ID, ctx.deviceID))
}

func (s *Server) childrenComplete(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	var req syncmodel.FolderChildrenCompleteRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		req.DeviceID = ctx.deviceID
	}
	if err := s.store.MarkChildrenReported(ctx.account.ID, req.DeviceID, req.RootPath); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) listFiles(w http.ResponseWriter, _ *http.Request, ctx syncContext) {
	writeJSON(w, http.StatusOK, syncmodel.ListResponse{Files: s.store.ListFiles(ctx.account.ID, ctx.spaceID)})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	after := int64(0)
	if v := r.URL.Query().Get("after"); v != "" {
		_, _ = fmtSscanf(v, &after)
	}
	writeJSON(w, http.StatusOK, syncmodel.EventsResponse{Events: s.store.EventsAfter(ctx.account.ID, ctx.spaceID, after)})
}

func (s *Server) checkChunks(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	var req syncmodel.CheckChunksRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, syncmodel.CheckChunksResponse{Missing: s.store.CheckChunks(ctx.account.ID, req.Chunks)})
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	hash := strings.TrimPrefix(r.URL.Path, "/v1/chunks/")
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 512<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	if err := s.store.PutChunk(hash, data); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	if err := s.store.GrantAccountChunk(ctx.account.ID, hash); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	hash := strings.TrimPrefix(r.URL.Path, "/v1/chunks/")
	if !s.store.HasAccountChunk(ctx.account.ID, hash) {
		writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "chunk is not available to this account"})
		return
	}
	p, err := s.store.ChunkPath(hash)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	http.ServeFile(w, r, p)
}

func (s *Server) commit(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	var req syncmodel.CommitRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, err := s.store.Commit(ctx.account.ID, ctx.spaceID, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request, ctx syncContext) {
	var req syncmodel.DeleteRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, err := s.store.Delete(ctx.account.ID, ctx.spaceID, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) clientLogin(w http.ResponseWriter, r *http.Request) {
	var req syncmodel.ClientLoginRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	account, token, storageRoot, err := s.store.IssueClientToken(req.Identifier, req.Password, req.DeviceID, req.Hostname, req.StorageRoot)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, syncmodel.ClientLoginResponse{
		Account:     accountProfile(account),
		Token:       token,
		SpaceID:     "default",
		SyncEnabled: account.SyncEnabled,
		Settings:    syncmodel.NormalizeSyncSettings(account.SyncSettings),
		StorageRoot: storageRoot,
	})
}

func (s *Server) clientStatus(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	deviceID := strings.TrimSpace(r.Header.Get("X-XCloud-Device"))
	writeJSON(w, http.StatusOK, s.store.ClientStatus(account.ID, deviceID))
}

func (s *Server) clientSyncToggle(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	var req syncmodel.ClientSyncToggleRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.store.SetAccountSyncEnabled(account.ID, req.Enabled); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	deviceID := strings.TrimSpace(r.Header.Get("X-XCloud-Device"))
	writeJSON(w, http.StatusOK, s.store.ClientStatus(account.ID, deviceID))
}

func (s *Server) createSyncRecord(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	var req syncmodel.SyncRecordRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	record, err := s.store.AddSyncRecord(account.ID, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	account, ok := s.sessionAccount(r)
	if !ok {
		mode := "login"
		if r.URL.Query().Get("view") == "register" {
			mode = "register"
		}
		s.renderAuth(w, authPageData{Mode: mode})
		return
	}
	s.renderDashboard(w, *account, flashMessage{})
}

func (s *Server) adminLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderAuth(w, authPageData{Mode: "login", Message: err.Error(), Kind: "error"})
		return
	}
	account, ok := s.store.AuthenticatePassword(r.Form.Get("identifier"), r.Form.Get("password"))
	if !ok {
		s.renderAuth(w, authPageData{Mode: "login", Message: "账号或密码错误", Kind: "error"})
		return
	}
	s.setSession(w, account.ID)
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (s *Server) adminRegister(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderAuth(w, authPageData{Mode: "register", Message: err.Error(), Kind: "error"})
		return
	}
	password := r.Form.Get("password")
	if password != r.Form.Get("confirm_password") {
		s.renderAuth(w, authPageData{Mode: "register", Message: "两次输入的密码不一致", Kind: "error"})
		return
	}
	account, token, err := s.store.CreateAccount(
		r.Form.Get("username"),
		r.Form.Get("email"),
		r.Form.Get("display_name"),
		password,
		false,
	)
	if err != nil {
		s.renderAuth(w, authPageData{Mode: "register", Message: err.Error(), Kind: "error"})
		return
	}
	s.setSession(w, account.ID)
	s.renderDashboard(w, account, flashMessage{
		Kind: "success",
		Text: fmt.Sprintf("账号已创建并登录。同步 token 只显示一次：%s", token),
	})
}

func (s *Server) adminLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		s.mu.Lock()
		delete(s.sessions, cookie.Value)
		s.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (s *Server) adminCreateAccount(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	created, token, err := s.store.CreateAccount(
		r.Form.Get("username"),
		r.Form.Get("email"),
		r.Form.Get("display_name"),
		r.Form.Get("password"),
		r.Form.Get("admin") == "on",
	)
	if err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: fmt.Sprintf("账号 %s 已创建。同步 token 只显示一次：%s", created.Username, token)})
}

func (s *Server) adminResetToken(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限重置其他账号 token"})
		return
	}
	target, ok := s.store.GetAccount(accountID)
	if !ok {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "账号不存在"})
		return
	}
	token, err := s.store.ResetSyncToken(accountID)
	if err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: fmt.Sprintf("账号 %s 的同步 token 已重置，只显示一次：%s", target.Username, token)})
}

func (s *Server) adminToggleAccount(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == account.ID {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "不能禁用当前登录账号"})
		return
	}
	if err := s.store.SetAccountDisabled(accountID, r.Form.Get("disabled") == "true"); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "账号状态已更新"})
}

func (s *Server) adminChangePassword(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	if r.Form.Get("new_password") != r.Form.Get("confirm_password") {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "两次输入的新密码不一致"})
		return
	}
	if err := s.store.ChangePassword(account.ID, r.Form.Get("current_password"), r.Form.Get("new_password")); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "登录密码已更新"})
}

func (s *Server) adminCreateSpace(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	ownerID := account.ID
	if account.IsAdmin && r.Form.Get("account_id") != "" {
		ownerID = r.Form.Get("account_id")
	}
	if ownerID != account.ID {
		if _, ok := s.store.GetAccount(ownerID); !ok {
			s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "目标账号不存在"})
			return
		}
	}
	if ownerID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限为其他账号创建 Space"})
		return
	}
	space, err := s.store.CreateSpace(ownerID, r.Form.Get("name"), r.Form.Get("description"))
	if err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "spaces"})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: fmt.Sprintf("Space %s 已创建。客户端使用 -space %s 时会同步 xcloud/%s。", space.Name, space.ID, space.ID)}, dashboardOptions{View: "spaces"})
}

func (s *Server) adminToggleSpace(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限修改其他账号 Space"}, dashboardOptions{View: "spaces"})
		return
	}
	if err := s.store.SetSpaceActive(accountID, r.Form.Get("space_id"), r.Form.Get("active") == "true"); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "spaces"})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "Space 状态已更新"}, dashboardOptions{View: "spaces"})
}

func (s *Server) adminSetClientStorageRoot(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "spaces"})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限修改其他账号的客户端保存目录"}, dashboardOptions{View: "spaces"})
		return
	}
	if err := s.store.SetClientStorageRoot(accountID, r.Form.Get("device_id"), r.Form.Get("storage_root")); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "spaces"})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "客户端全局 xcloud 保存目录已更新，客户端下一轮状态刷新后生效"}, dashboardOptions{View: "spaces"})
}

func (s *Server) adminRestoreTrash(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "trash"})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限恢复其他账号的文件"}, dashboardOptions{View: "trash"})
		return
	}
	req := syncmodel.RestoreRequest{
		SpaceID: r.Form.Get("space_id"),
		FileID:  r.Form.Get("file_id"),
		Path:    r.Form.Get("path"),
	}
	if _, err := s.store.RestoreTrash(accountID, req, account.Username); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "trash"})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "文件已从云端回收站恢复，所有客户端下一轮同步会恢复该文件"}, dashboardOptions{View: "trash"})
}

func (s *Server) adminUpdateSyncSettings(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "settings"})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限修改其他账号的同步规则"}, dashboardOptions{View: "settings"})
		return
	}
	settings := syncmodel.SyncSettings{
		RealtimeEnabled: r.Form.Get("realtime_enabled") == "on",
		DebounceMillis:  atoiDefault(r.Form.Get("debounce_millis"), syncmodel.DefaultSyncDebounceMillis),
		IntervalSeconds: atoiDefault(r.Form.Get("interval_seconds"), syncmodel.DefaultSyncIntervalSeconds),
	}
	if err := s.store.SetSyncSettings(accountID, settings); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "settings"})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "同步触发规则已更新"}, dashboardOptions{View: "settings"})
}

func (s *Server) adminUpdateServerConfig(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "server"})
		return
	}
	cfg := s.runtimeConfig
	cfg.Domain = r.Form.Get("domain")
	cfg.Port = atoiDefault(r.Form.Get("port"), 18002)
	cfg.DataDir = r.Form.Get("data_dir")
	cfg.ListenHost = r.Form.Get("listen_host")
	cfg.Normalize()
	cfg.Path = s.runtimeConfig.Path
	if err := WriteRuntimeConfigFile(cfg); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "server"})
		return
	}
	if err := s.store.SaveRuntimeConfig(cfg); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()}, dashboardOptions{View: "server"})
		return
	}
	s.runtimeConfig = cfg
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "服务配置已保存。域名立即用于页面提示；端口和 data 目录需要重启服务端进程后生效。"}, dashboardOptions{View: "server"})
}

func (s *Server) requireLogin(next func(http.ResponseWriter, *http.Request, syncmodel.Account)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		account, ok := s.sessionAccount(r)
		if !ok {
			http.Redirect(w, r, "/admin", http.StatusFound)
			return
		}
		next(w, r, *account)
	}
}

func (s *Server) requireAdmin(next func(http.ResponseWriter, *http.Request, syncmodel.Account)) http.HandlerFunc {
	return s.requireLogin(func(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
		if !account.IsAdmin {
			s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "需要管理员权限"})
			return
		}
		next(w, r, account)
	})
}

func (s *Server) sessionAccount(r *http.Request) (*syncmodel.Account, bool) {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || cookie.Value == "" {
		return nil, false
	}
	s.mu.Lock()
	accountID := s.sessions[cookie.Value]
	s.mu.Unlock()
	if accountID == "" {
		return nil, false
	}
	return s.store.GetAccount(accountID)
}

func (s *Server) setSession(w http.ResponseWriter, accountID string) {
	sessionID := fileutil.NewID() + fileutil.NewID()
	s.mu.Lock()
	s.sessions[sessionID] = accountID
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(24 * time.Hour),
	})
}

type authPageData struct {
	Mode    string
	Message string
	Kind    string
}

type flashMessage struct {
	Kind string
	Text string
}

func (f flashMessage) HasText() bool {
	return f.Text != ""
}

type dashboardOptions struct {
	View string
}

func accountProfile(account syncmodel.Account) syncmodel.AccountProfile {
	return syncmodel.AccountProfile{
		ID:          account.ID,
		Username:    account.Username,
		DisplayName: account.DisplayName,
		Email:       account.Email,
		IsAdmin:     account.IsAdmin,
	}
}

func (s *Server) renderAuth(w http.ResponseWriter, data authPageData) {
	if data.Mode == "" {
		data.Mode = "login"
	}
	page := template.Must(template.New("auth").Parse(authHTML))
	_ = page.Execute(w, data)
}

func (s *Server) renderDashboard(w http.ResponseWriter, account syncmodel.Account, flash flashMessage, opts ...dashboardOptions) {
	options := dashboardOptions{View: "overview"}
	if len(opts) > 0 {
		options = opts[0]
	}
	if options.View == "" {
		options.View = "overview"
	}
	accounts := []syncmodel.Account{account}
	if account.IsAdmin {
		accounts = s.store.ListAccounts()
	}
	type spaceGroup struct {
		Account           syncmodel.Account
		Spaces            []syncmodel.SpaceSummary
		Devices           []syncmodel.ClientDevice
		HasDuplicateNames bool
		DuplicateNames    []string
		Trash             []syncmodel.TrashEntry
		Records           []syncmodel.SyncRecord
		Settings          syncmodel.SyncSettings
	}
	groups := make([]spaceGroup, 0, len(accounts))
	totalSpaces := 0
	totalFiles := 0
	totalDeleted := 0
	activeSpaces := 0
	for _, item := range accounts {
		spaces := s.store.ListSpaces(item.ID)
		devices := s.store.ListClientDevices(item.ID)
		deviceNames := map[string]int{}
		for _, device := range devices {
			name := strings.TrimSpace(device.Hostname)
			if name == "" {
				name = device.DeviceID
			}
			deviceNames[name]++
		}
		duplicateNames := []string{}
		for name, count := range deviceNames {
			if count > 1 {
				duplicateNames = append(duplicateNames, name)
			}
		}
		sort.Strings(duplicateNames)
		for _, summary := range spaces {
			totalSpaces++
			totalFiles += summary.FileCount
			totalDeleted += summary.Deleted
			if summary.Space.Active {
				activeSpaces++
			}
		}
		groups = append(groups, spaceGroup{
			Account:           item,
			Spaces:            spaces,
			Devices:           devices,
			HasDuplicateNames: len(duplicateNames) > 0,
			DuplicateNames:    duplicateNames,
			Trash:             s.store.ListTrash(item.ID, 200),
			Records:           s.store.ListSyncRecords(item.ID, 120),
			Settings:          syncmodel.NormalizeSyncSettings(item.SyncSettings),
		})
	}
	page := template.Must(template.New("dashboard").Funcs(template.FuncMap{
		"formatTime": formatUnixTime,
	}).Parse(dashboardHTML))
	_ = page.Execute(w, map[string]any{
		"Account":        account,
		"Accounts":       accounts,
		"Groups":         groups,
		"Flash":          flash,
		"TotalAccounts":  len(accounts),
		"TotalSpaces":    totalSpaces,
		"ActiveSpaces":   activeSpaces,
		"TotalFiles":     totalFiles,
		"TotalDeleted":   totalDeleted,
		"InitialView":    options.View,
		"RuntimeConfig":  s.runtimeConfig,
		"PublicURL":      s.runtimeConfig.PublicURL(),
		"CurrentDataDir": s.store.Root(),
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func fmtSscanf(v string, out *int64) (int, error) {
	var n int64
	sign := int64(1)
	for i, r := range v {
		if i == 0 && r == '-' {
			sign = -1
			continue
		}
		if r < '0' || r > '9' {
			return 0, nil
		}
		n = n*10 + int64(r-'0')
	}
	*out = n * sign
	return 1, nil
}

func atoiDefault(v string, fallback int) int {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	n := 0
	for _, r := range v {
		if r < '0' || r > '9' {
			return fallback
		}
		n = n*10 + int(r-'0')
	}
	if n == 0 {
		return fallback
	}
	return n
}

func formatUnixTime(ts int64) string {
	if ts <= 0 {
		return "-"
	}
	return time.Unix(ts, 0).Format("2006-01-02 15:04:05")
}

const authHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>xcloud 管理后台</title>
  <style>
    :root{--ink:#000;--canvas:#fff;--soft:#f7f7f5;--hair:#e6e6e6;--lime:#dceeb1;--lilac:#c5b0f4;--cream:#f4ecd6;--pink:#efd4d4;--mint:#c8e6cd;--coral:#f3c9b6;--navy:#1f1d3d}
    *{box-sizing:border-box}body{margin:0;font-family:figmaSans,Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;background:var(--canvas);color:var(--ink)}
    .page{min-height:100vh;display:grid;grid-template-columns:minmax(0,1fr) 440px;background:var(--canvas)}
    .visual{position:relative;padding:32px 48px 48px;display:flex;flex-direction:column;justify-content:space-between;border-right:1px solid var(--hair);overflow:hidden}
    .visual:before{content:"";position:absolute;right:48px;top:112px;width:min(560px,58vw);height:360px;background:var(--lime);border-radius:24px;transform:rotate(-2deg);z-index:0}.visual:after{content:"";position:absolute;left:48px;bottom:42px;width:260px;height:170px;background:var(--lilac);border-radius:24px;transform:rotate(2deg);z-index:0}
    .brand,.hero,.metrics{position:relative;z-index:1}.brand{display:flex;align-items:center;gap:12px;font-weight:700;font-size:16px}.logo{width:36px;height:36px;border-radius:9999px;background:var(--ink);color:var(--canvas);display:flex;align-items:center;justify-content:center;font-weight:800}
    .hero{max-width:760px;margin:72px 0}.hero h1{font-size:64px;line-height:1.02;margin:0 0 24px;letter-spacing:0;font-weight:340}.hero p{font-size:20px;line-height:1.4;max-width:620px;margin:0;font-weight:330}
    .metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;max-width:760px}.metric{border:1px solid var(--ink);border-radius:24px;padding:20px;background:var(--canvas)}.metric:nth-child(1){background:var(--cream)}.metric:nth-child(2){background:var(--mint)}.metric:nth-child(3){background:var(--pink)}.metric strong{display:block;font-size:24px;font-weight:700}.metric span{font-size:13px;line-height:1.35}
    .panel{display:flex;align-items:center;justify-content:center;padding:28px;background:var(--cream)}
    .card{width:100%;max-width:388px;background:var(--canvas);border:1px solid var(--ink);border-radius:24px;padding:28px;box-shadow:8px 8px 0 var(--ink)}
    .tabs{display:flex;gap:6px;border:1px solid var(--hair);border-radius:9999px;padding:4px;margin-bottom:28px;background:var(--soft)}.tabs a{flex:1;height:38px;display:flex;align-items:center;justify-content:center;border-radius:9999px;text-decoration:none;color:var(--ink);font-weight:480;font-size:16px}.tabs a.active{background:var(--ink);color:var(--canvas)}
    h2{margin:0 0 8px;font-size:32px;letter-spacing:0;font-weight:540}.sub{margin:0 0 22px;color:var(--ink);font-size:16px;line-height:1.45;font-weight:330}.msg{border-radius:8px;padding:12px 14px;margin-bottom:16px;font-size:14px;line-height:1.45;border:1px solid var(--ink)}.msg.error{background:var(--pink)}.msg.success{background:var(--mint)}
    label{display:block;font-size:12px;font-weight:400;margin:14px 0 7px;font-family:figmaMono,"SF Mono",Menlo,monospace;text-transform:uppercase;letter-spacing:.6px}.field{position:relative}.field input{width:100%;height:48px;border:1px solid var(--hair);background:var(--canvas);color:var(--ink);border-radius:8px;padding:0 58px 0 44px;font-size:18px;outline:none}.field input:focus{border-color:var(--ink);box-shadow:0 0 0 3px var(--lime)}.ico{position:absolute;left:14px;top:50%;transform:translateY(-50%);font-size:12px;font-family:figmaMono,"SF Mono",Menlo,monospace}.toggle{position:absolute;right:8px;top:50%;transform:translateY(-50%);height:32px;border:0;background:transparent;color:var(--ink);cursor:pointer;font-size:13px}
    .row{display:flex;justify-content:space-between;align-items:center;margin:18px 0 22px;font-size:13px}.row label{margin:0;font-family:inherit;text-transform:none;letter-spacing:0}.row input{vertical-align:middle;margin-right:6px}.link{color:var(--ink);text-decoration:underline;text-underline-offset:3px;border:0;background:transparent;cursor:pointer;font:inherit}
    .submit{width:100%;height:48px;border:0;border-radius:9999px;background:var(--ink);color:var(--canvas);font-weight:480;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:8px}.submit:hover{transform:translateY(-1px)}.foot{margin:22px 0 0;text-align:center;font-size:14px}
    .hint{margin-top:18px;font-size:12px;line-height:1.5}.hidden{display:none}
    @media(max-width:920px){.page{grid-template-columns:1fr}.visual{display:none}.panel{min-height:100vh;border:0}.card{box-shadow:5px 5px 0 var(--ink)}}@media(max-width:560px){.panel{padding:18px}.card{padding:22px}.hero h1{font-size:44px}}
  </style>
</head>
<body>
  <div class="page">
    <section class="visual">
      <div class="brand"><div class="logo">X</div><span>xcloud 控制台</span></div>
      <div class="hero">
        <h1>账号隔离的文件同步中心</h1>
        <p>管理账号、同步 token、客户端、Space 和云端回收站。每个账号的数据只在自己的 Space 内流转，服务端负责版本协调、删除保留和 chunk 校验。</p>
      </div>
      <div class="metrics">
        <div class="metric"><strong>SHA-256</strong><span>端到端完整性校验</span></div>
        <div class="metric"><strong>Space</strong><span>账号内项目隔离</span></div>
        <div class="metric"><strong>Token</strong><span>客户端同步凭证</span></div>
      </div>
    </section>
    <section class="panel">
      <div class="card">
        <div class="tabs">
          <a href="/admin?view=login" class="{{if eq .Mode "login"}}active{{end}}">登录</a>
          <a href="/admin?view=register" class="{{if eq .Mode "register"}}active{{end}}">注册</a>
        </div>
        {{if .Message}}<div class="msg {{.Kind}}">{{.Message}}</div>{{end}}
        {{if eq .Mode "register"}}
        <h2>创建账号</h2>
        <p class="sub">注册后会自动登录，并创建默认 Space。</p>
        <form method="post" action="/admin/register">
          <label>用户名</label>
          <div class="field"><span class="ico">ID</span><input name="username" autocomplete="username" placeholder="用户名" required></div>
          <label>显示名</label>
          <div class="field"><span class="ico">NM</span><input name="display_name" autocomplete="name" placeholder="显示名称"></div>
          <label>邮箱</label>
          <div class="field"><span class="ico">@</span><input name="email" type="email" autocomplete="email" placeholder="邮箱地址"></div>
          <label>密码</label>
          <div class="field"><span class="ico">PW</span><input id="reg_password" name="password" type="password" autocomplete="new-password" placeholder="至少 8 位" required><button class="toggle" type="button" data-toggle="reg_password">显示</button></div>
          <label>确认密码</label>
          <div class="field"><span class="ico">PW</span><input id="confirm_password" name="confirm_password" type="password" autocomplete="new-password" required><button class="toggle" type="button" data-toggle="confirm_password">显示</button></div>
          <button class="submit" type="submit">注册并进入 →</button>
        </form>
        <p class="foot">已有账号？<a class="link" href="/admin?view=login">返回登录</a></p>
        {{else}}
        <h2>欢迎回来</h2>
        <p class="sub">使用用户名或邮箱登录管理你的客户端、Space 和回收站。</p>
        <form method="post" action="/admin/login">
          <label>账号或邮箱</label>
          <div class="field"><span class="ico">ID</span><input name="identifier" autocomplete="username" placeholder="用户名或邮箱" required></div>
          <label>密码</label>
          <div class="field"><span class="ico">PW</span><input id="login_password" name="password" type="password" autocomplete="current-password" required><button class="toggle" type="button" data-toggle="login_password">显示</button></div>
          <div class="row"><label><input type="checkbox" name="remember"> 保持登录</label><span>默认 24 小时会话</span></div>
          <button class="submit" type="submit">登录 →</button>
        </form>
        <p class="foot">没有账号？<a class="link" href="/admin?view=register">立即注册</a></p>
        {{end}}
      </div>
    </section>
  </div>
  <script>
    document.querySelectorAll("[data-toggle]").forEach(function(btn){
      btn.addEventListener("click",function(){
        var input=document.getElementById(btn.getAttribute("data-toggle"));
        input.type=input.type==="password"?"text":"password";
        btn.textContent=input.type==="password"?"显示":"隐藏";
      });
    });
  </script>
</body>
</html>`

const dashboardHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>xcloud 管理后台</title>
  <style>
    :root{--ink:#000;--canvas:#fff;--inverse:#000;--inverse-ink:#fff;--hair:#e6e6e6;--soft:#f7f7f5;--lime:#dceeb1;--lilac:#c5b0f4;--cream:#f4ecd6;--pink:#efd4d4;--mint:#c8e6cd;--coral:#f3c9b6;--navy:#1f1d3d;--magenta:#ff3d8b}
    *{box-sizing:border-box}body{margin:0;font-family:figmaSans,Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;background:var(--canvas);color:var(--ink)}
    .shell{min-height:100vh;display:flex}.side{width:268px;background:var(--canvas);color:var(--ink);display:flex;flex-direction:column;position:fixed;inset:0 auto 0 0;border-right:1px solid var(--hair);z-index:20}.scrim{display:none}.brand{height:72px;display:flex;align-items:center;gap:12px;padding:0 24px;border-bottom:1px solid var(--hair)}.logo{width:36px;height:36px;border-radius:9999px;background:var(--ink);color:var(--canvas);display:flex;align-items:center;justify-content:center;font-weight:800}.brand b{font-size:18px}.brand span{font-weight:330}
    .nav{padding:20px 14px;flex:1}.nav p{font-size:12px;text-transform:uppercase;letter-spacing:.6px;font-family:figmaMono,"SF Mono",Menlo,monospace;margin:10px 12px 14px}.nav button{width:100%;height:44px;display:flex;align-items:center;gap:10px;padding:0 14px;border-radius:9999px;color:var(--ink);font-size:16px;margin:6px 0;background:transparent;border:0;font-weight:480;cursor:pointer;text-align:left}.nav button.active{background:var(--ink);color:var(--canvas)}.nav button:not(.active):hover{background:var(--soft)}.logout{padding:18px;border-top:1px solid var(--hair)}.logout button{width:100%;height:42px;border:1px solid var(--ink);border-radius:9999px;background:var(--canvas);color:var(--ink);font-weight:480;cursor:pointer}
    .main{margin-left:268px;min-width:0;flex:1}.top{height:72px;background:var(--canvas);border-bottom:1px solid var(--hair);display:flex;align-items:center;justify-content:space-between;padding:0 32px;position:sticky;top:0;z-index:5}.top-left{display:flex;align-items:center;gap:14px;min-width:0}.menu-toggle{display:none;width:42px;height:42px;padding:0;align-items:center;justify-content:center}.search{height:42px;width:min(420px,48vw);display:flex;align-items:center;gap:8px;background:var(--soft);border:1px solid var(--hair);border-radius:9999px;padding:0 16px}.search input{border:0;background:transparent;outline:none;width:100%;font-size:16px}.user{display:flex;align-items:center;gap:12px}.avatar{width:38px;height:38px;border-radius:9999px;background:var(--lilac);color:var(--ink);display:flex;align-items:center;justify-content:center;font-weight:700;border:1px solid var(--ink)}.content{padding:32px;max-width:1320px;margin:0 auto}.hero{display:flex;justify-content:space-between;gap:24px;align-items:flex-start;margin-bottom:24px;padding:48px;border-radius:24px;background:var(--lime)}.hero h1{font-size:64px;line-height:1;margin:0 0 14px;letter-spacing:0;font-weight:340}.muted{color:var(--ink);font-size:16px;line-height:1.45;font-weight:330}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px}.stat{border:1px solid var(--ink);border-radius:24px;padding:22px;background:var(--canvas)}.stat:nth-child(2){background:var(--cream)}.stat:nth-child(3){background:var(--mint)}.stat:nth-child(4){background:var(--pink)}.stat .label{font-size:12px;text-transform:uppercase;letter-spacing:.6px;font-family:figmaMono,"SF Mono",Menlo,monospace;margin-bottom:10px}.stat strong{font-size:36px;font-weight:540}.stat .hint{font-size:13px;margin-top:10px}
    .two{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(320px,.8fr);gap:18px;margin-top:18px}.panel{background:var(--canvas);border:1px solid var(--hair);border-radius:24px;padding:24px;margin-bottom:18px}.panel:nth-child(2n){background:var(--cream)}.panel h2{font-size:26px;margin:0 0 14px;font-weight:540}.panel h3{font-size:20px;margin:20px 0 10px}.flash{border-radius:8px;padding:14px 16px;margin-bottom:18px;word-break:break-all;font-size:15px;border:1px solid var(--ink)}.flash.success{background:var(--mint)}.flash.error{background:var(--pink)}
    .mini{font-size:12px;line-height:1.35}.warn{background:var(--coral);color:var(--ink)}.empty-tree{padding:18px}
    .duplicate-alert{border:1px solid var(--ink);border-radius:8px;background:var(--coral);padding:10px 12px;margin:10px 0;font-size:13px;line-height:1.45}.device-list{display:grid;gap:10px}.device-card{border:1px solid var(--ink);border-radius:8px;background:var(--canvas);padding:14px}.device-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:8px}.device-title{min-width:0}.device-title strong{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.device-form{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:end}.device-form button{height:42px}.device-meta{font-size:12px;line-height:1.4;margin-top:6px;word-break:break-all}
    .record-list{display:grid;gap:8px}.record-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;border:1px solid var(--hair);border-radius:8px;padding:12px;background:var(--canvas)}.record-main{min-width:0}.record-title{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.record-path{font-size:12px;word-break:break-all;margin-top:4px}.record-meta{font-size:12px;margin-top:5px}.record-side{text-align:right;font-size:12px;min-width:160px}
    table{width:100%;border-collapse:collapse;font-size:15px}th,td{border-bottom:1px solid var(--hair);text-align:left;padding:12px 8px;vertical-align:top}th{font-size:12px;text-transform:uppercase;letter-spacing:.6px;font-family:figmaMono,"SF Mono",Menlo,monospace}code,.cmd{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.cmd{background:var(--inverse);color:var(--inverse-ink);border-radius:8px;padding:14px;word-break:break-all}
    label{display:block;font-size:12px;font-weight:400;margin:12px 0 6px;font-family:figmaMono,"SF Mono",Menlo,monospace;text-transform:uppercase;letter-spacing:.6px}input,textarea,select{box-sizing:border-box;width:100%;border:1px solid var(--hair);border-radius:8px;padding:11px 12px;font-size:16px;background:var(--canvas);color:var(--ink)}input:focus,textarea:focus,select:focus{outline:none;border-color:var(--ink);box-shadow:0 0 0 3px var(--lime)}textarea{min-height:78px;resize:vertical}.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.inline{display:inline}
    button{border:0;border-radius:9999px;background:var(--ink);color:var(--canvas);font-weight:480;font-size:15px;padding:10px 18px;cursor:pointer}.secondary{background:var(--canvas);color:var(--ink);border:1px solid var(--ink)}.danger{background:var(--magenta);color:var(--canvas)}.badge{display:inline-flex;align-items:center;min-height:24px;padding:0 9px;border-radius:9999px;font-size:12px;font-weight:480;background:var(--canvas);color:var(--ink);border:1px solid var(--ink)}.ok{background:var(--mint);color:var(--ink)}.off{background:var(--pink);color:var(--ink)}
    .view{display:none}.view.active{display:block}.overview-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.compact{max-width:760px}.section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:18px}.small-select{min-width:140px}.path{max-width:360px;word-break:break-all}
    @media(max-width:980px){.side{width:min(300px,86vw);transform:translateX(-102%);transition:transform .22s ease}.shell.menu-open .side{transform:translateX(0)}.scrim{position:fixed;inset:0;background:rgba(0,0,0,.48);z-index:15}.shell.menu-open .scrim{display:block}.main{margin-left:0}.menu-toggle{display:inline-flex}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.two{grid-template-columns:1fr}.top{padding:0 16px}.content{padding:18px}.hero{flex-direction:column;padding:32px}.hero h1{font-size:48px}}@media(max-width:560px){.grid,.form-grid,.device-form{grid-template-columns:1fr}.search{display:none}.hero h1{font-size:40px}.user .muted{display:none}}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="side">
      <div class="brand"><div class="logo">X</div><b>xcloud<span>Admin</span></b></div>
      <nav class="nav">
        <p>主菜单</p>
        <button class="active" type="button" data-view-target="overview">▣ 概览</button>
        <button type="button" data-view-target="spaces">▤ Space 与设备</button>
        <button type="button" data-view-target="trash">♲ 云端回收站</button>
        <button type="button" data-view-target="records">◴ 同步记录</button>
        {{if .Account.IsAdmin}}<button type="button" data-view-target="accounts">◎ 账号管理</button>{{end}}
        <button type="button" data-view-target="settings">⚙ 同步设置</button>
        {{if .Account.IsAdmin}}<button type="button" data-view-target="server">◉ 服务配置</button>{{end}}
        <button type="button" data-view-target="security">⚿ 安全设置</button>
      </nav>
      <div class="logout"><form method="post" action="/admin/logout"><button type="submit">退出登录</button></form></div>
    </aside>
    <div class="scrim" data-menu-close></div>
    <div class="main">
      <header class="top">
        <div class="top-left"><button class="secondary menu-toggle" type="button" data-menu-toggle aria-label="打开菜单">☰</button><div class="search">⌕ <input placeholder="搜索账号、客户端或 Space ID"></div></div>
        <div class="user"><span class="muted">{{.Account.DisplayName}} / {{.Account.Username}}{{if .Account.IsAdmin}} / 管理员{{end}}</span><div class="avatar">{{printf "%.1s" .Account.Username}}</div></div>
      </header>
      <main class="content">
        {{if .Flash.HasText}}<div class="flash {{.Flash.Kind}}">{{.Flash.Text}}</div>{{end}}
        <section class="view active" data-view="overview">
          <div class="hero">
            <div>
              <h1>同步控制台</h1>
              <p class="muted">客户端只同步本机 xcloud 保存根目录下对应 Space 的文件。新增、编辑、删除都会同步到同账号同 Space 的所有客户端。</p>
            </div>
            <div class="overview-actions">
              <button type="button" class="secondary" data-view-target="spaces">查看 Space 与设备</button>
              <form method="post" action="/admin/accounts/reset-token">
                <input type="hidden" name="account_id" value="{{.Account.ID}}">
                <button type="submit">重置我的同步 token</button>
              </form>
            </div>
          </div>
          <div class="grid">
            <div class="stat"><div class="label">账号数</div><strong>{{.TotalAccounts}}</strong><div class="hint">当前可管理范围</div></div>
            <div class="stat"><div class="label">Space</div><strong>{{.TotalSpaces}}</strong><div class="hint">{{.ActiveSpaces}} 个启用中</div></div>
            <div class="stat"><div class="label">有效文件</div><strong>{{.TotalFiles}}</strong><div class="hint">当前云端有效版本</div></div>
            <div class="stat"><div class="label">回收站</div><strong>{{.TotalDeleted}}</strong><div class="hint">删除文件保留 10 天</div></div>
          </div>
          <section class="panel" style="margin-top:18px">
            <h2>客户端命令</h2>
            <p class="muted">客户端首次启动无需 token。打开本机客户端页面登录云端账号并开启同步后，只要文件放在 xcloud 目录下就会参与同步。</p>
            <div class="cmd">mkdir -p ./xcloud && ./xcloud client -server {{.PublicURL}}</div>
            <p class="muted">默认保存根目录是客户端进程启动目录下的 xcloud。默认 Space 对应 xcloud/default；其他 Space 对应 xcloud/&lt;space-id&gt;。</p>
          </section>
        </section>

        <section class="view" data-view="spaces">
          <div class="two">
            <div>
            {{range .Groups}}
            <section class="panel">
              <div class="section-head">
                <div>
                  <h2>{{.Account.Username}} 的 Space</h2>
                  <p class="muted">Space 仍然保留，用来隔离不同项目。每个客户端只同步 xcloud 目录下对应 Space 的子目录。</p>
                </div>
              </div>
              <table>
                <thead><tr><th>名称</th><th>ID</th><th>本地路径</th><th>文件</th><th>回收站</th><th>状态</th><th>操作</th></tr></thead>
                <tbody>
                  {{range .Spaces}}
                  <tr>
                    <td>{{.Space.Name}}</td>
                    <td><code>{{.Space.ID}}</code></td>
                    <td><code>xcloud/{{.Space.ID}}</code></td>
                    <td>{{.FileCount}}</td>
                    <td>{{.Trash}}</td>
                    <td>{{if .Space.Active}}<span class="badge ok">启用</span>{{else}}<span class="badge off">停用</span>{{end}}</td>
                    <td>
                      <form class="inline" method="post" action="/admin/spaces/toggle">
                        <input type="hidden" name="account_id" value="{{.Space.AccountID}}">
                        <input type="hidden" name="space_id" value="{{.Space.ID}}">
                        {{if .Space.Active}}
                        <input type="hidden" name="active" value="false"><button class="secondary" type="submit">停用</button>
                        {{else}}
                        <input type="hidden" name="active" value="true"><button type="submit">启用</button>
                        {{end}}
                      </form>
                    </td>
                  </tr>
                  {{else}}
                  <tr><td colspan="7"><span class="muted">暂无 Space。</span></td></tr>
                  {{end}}
                </tbody>
              </table>
            </section>
            {{end}}
            <section class="panel">
              <h2>创建 Space</h2>
              <form method="post" action="/admin/spaces/create">
                {{if .Account.IsAdmin}}
                <label>所属账号</label>
                <select name="account_id">{{range .Accounts}}<option value="{{.ID}}">{{.Username}}</option>{{end}}</select>
                {{end}}
                <label>名称</label>
                <input name="name" placeholder="docs 或 project-a" required>
                <label>说明</label>
                <textarea name="description" placeholder="可选"></textarea>
                <button type="submit">创建 Space</button>
              </form>
            </section>
            </div>
            <aside>
            {{range .Groups}}
            <section class="panel">
              <h2>{{.Account.Username}} 的客户端</h2>
              <p class="muted">这里配置的是设备级全局 xcloud 根目录。该目录下的每个 Space 子目录会保持一致。</p>
              {{if .HasDuplicateNames}}
              <div class="duplicate-alert">检测到客户端名称重复：{{range .DuplicateNames}}<code>{{.}}</code> {{end}}。建议启动客户端时使用 <code>-device</code> 指定唯一名称。</div>
              {{end}}
              <div class="device-list">
                {{range .Devices}}
                <div class="device-card">
                  <div class="device-head">
                    <div class="device-title"><strong>{{.DeviceID}}</strong><span class="mini">{{if .Hostname}}{{.Hostname}}{{else}}未上报主机名{{end}}</span></div>
                    <span class="badge">{{if .LastSeenAt}}{{formatTime .LastSeenAt}}{{else}}未心跳{{end}}</span>
                  </div>
                  <form class="device-form" method="post" action="/admin/clients/storage-root">
                    <input type="hidden" name="account_id" value="{{.AccountID}}">
                    <input type="hidden" name="device_id" value="{{.DeviceID}}">
                    <div>
                      <label>保存根目录</label>
                      <input name="storage_root" value="{{.StorageRoot}}" placeholder="/absolute/path/to/xcloud" required>
                    </div>
                    <button class="secondary" type="submit">保存</button>
                  </form>
                  <div class="device-meta">客户端会自动创建该目录；路径必须是客户端所在机器上的绝对路径。同步内容只来自该目录下的 Space 子目录。</div>
                </div>
                {{else}}
                <div class="empty-tree"><span class="muted">暂无已登录客户端。客户端登录云端账号后会出现在这里。</span></div>
                {{end}}
              </div>
            </section>
            {{end}}
            </aside>
          </div>
        </section>

        <section class="view" data-view="trash">
          {{range .Groups}}
          <section class="panel">
            <div class="section-head">
              <div>
                <h2>{{.Account.Username}} 的云端回收站</h2>
                <p class="muted">删除文件会在云端临时保留 10 天。恢复后会产生新的文件版本，并同步恢复到所有客户端。</p>
              </div>
            </div>
            <div class="record-list">
              {{range .Trash}}
              <div class="record-row">
                <div class="record-main">
                  <div class="record-title">
                    <strong>{{.Path}}</strong>
                    <span class="badge">{{.SpaceID}}</span>
                    <span class="badge off">已删除</span>
                  </div>
                  <div class="record-path">删除时间 {{formatTime .DeletedAt}} · 过期时间 {{formatTime .ExpiresAt}} · 大小 {{.Size}} bytes</div>
                </div>
                <div class="record-side">
                  <form method="post" action="/admin/trash/restore">
                    <input type="hidden" name="account_id" value="{{.AccountID}}">
                    <input type="hidden" name="space_id" value="{{.SpaceID}}">
                    <input type="hidden" name="file_id" value="{{.FileID}}">
                    <input type="hidden" name="path" value="{{.Path}}">
                    <button type="submit">恢复</button>
                  </form>
                </div>
              </div>
              {{else}}
              <div class="empty-tree"><span class="muted">回收站为空。</span></div>
              {{end}}
            </div>
          </section>
          {{end}}
        </section>

        <section class="view" data-view="records">
          {{range .Groups}}
          <section class="panel">
            <div class="section-head">
              <div>
                <h2>{{.Account.Username}} 的同步记录</h2>
                <p class="muted">记录最近的上传、下载、删除、冲突和失败操作。</p>
              </div>
            </div>
            <div class="record-list">
              {{range .Records}}
              <div class="record-row">
                <div class="record-main">
                  <div class="record-title">
                    <strong>{{.Action}}</strong>
                    {{if eq .Status "success"}}<span class="badge ok">成功</span>{{else}}<span class="badge off">失败</span>{{end}}
                    <span class="badge">{{.DeviceID}}</span>
                    {{if .SpaceID}}<span class="badge">{{.SpaceID}}</span>{{end}}
                  </div>
                  <div class="record-path">{{if .Path}}<code>{{.Path}}</code>{{else}}<span class="muted">无文件路径</span>{{end}}</div>
                  {{if .Error}}<div class="record-path"><span class="badge off">错误</span> {{.Error}}</div>{{end}}
                  <div class="record-meta">根目录 <code>{{.RootPath}}</code></div>
                </div>
                <div class="record-side">
                  <div>{{formatTime .CreatedAt}}</div>
                  <div>{{.DurationMillis}} ms</div>
                </div>
              </div>
              {{else}}
              <div class="empty-tree"><span class="muted">暂无同步记录。</span></div>
              {{end}}
            </div>
          </section>
          {{end}}
        </section>

        <section class="view" data-view="settings">
          <div class="two">
            {{range .Groups}}
            <section class="panel">
              <h2>{{.Account.Username}} 的同步触发规则</h2>
              <form method="post" action="/admin/sync-settings/update">
                <input type="hidden" name="account_id" value="{{.Account.ID}}">
                <label><input style="width:auto" type="checkbox" name="realtime_enabled" {{if .Settings.RealtimeEnabled}}checked{{end}}> 启用实时文件监听</label>
                <div class="form-grid">
                  <div>
                    <label>变更防抖毫秒</label>
                    <input name="debounce_millis" type="number" min="100" value="{{.Settings.DebounceMillis}}">
                  </div>
                  <div>
                    <label>兜底扫描间隔秒</label>
                    <input name="interval_seconds" type="number" min="1" value="{{.Settings.IntervalSeconds}}">
                  </div>
                </div>
                <button type="submit">保存同步规则</button>
              </form>
            </section>
            {{end}}
          </div>
        </section>

        {{if .Account.IsAdmin}}
        <section class="view" data-view="server">
          <div class="two">
            <section class="panel">
              <h2>服务配置</h2>
              <p class="muted">这里配置管理端对外地址、监听端口和服务端 data 目录。域名用于页面提示；端口和 data 目录需要重启服务端进程后生效。</p>
              <form method="post" action="/admin/server-config/update">
                <label>默认域名</label>
                <input name="domain" value="{{.RuntimeConfig.Domain}}" placeholder="ixxmi.com" required>
                <div class="form-grid">
                  <div>
                    <label>固定端口</label>
                    <input name="port" type="number" min="1" max="65535" value="{{.RuntimeConfig.Port}}" required>
                  </div>
                  <div>
                    <label>监听主机</label>
                    <input name="listen_host" value="{{.RuntimeConfig.ListenHost}}" placeholder="留空表示 0.0.0.0">
                  </div>
                </div>
                <label>data 目录</label>
                <input name="data_dir" value="{{.RuntimeConfig.DataDir}}" placeholder="server-data" required>
                <button type="submit">保存服务配置</button>
              </form>
            </section>
            <aside>
              <section class="panel">
                <h2>当前生效状态</h2>
                <p class="muted">当前对外地址</p>
                <div class="cmd">{{.PublicURL}}</div>
                <p class="muted">当前已打开的 data 目录</p>
                <div class="cmd">{{.CurrentDataDir}}</div>
                <p class="muted">如果修改端口或 data 目录，请使用自动重启脚本重启服务端进程。脚本会使用配置文件启动，不需要再传 <code>-addr</code> 或 <code>-data</code>。</p>
              </section>
            </aside>
          </div>
        </section>

        <section class="view" data-view="accounts">
          <div class="two">
            <section class="panel">
              <h2>账号管理</h2>
              <p class="muted">只有管理员账号可以查看、禁用、启用其他账号，或重置其他账号的同步 token。</p>
              <table>
                <thead><tr><th>账号</th><th>邮箱</th><th>角色</th><th>状态</th><th>操作</th></tr></thead>
                <tbody>
                  {{range .Accounts}}
                  <tr>
                    <td><strong>{{.Username}}</strong><br><span class="muted">{{.DisplayName}}</span></td>
                    <td>{{.Email}}</td>
                    <td>{{if .IsAdmin}}管理员{{else}}普通账号{{end}}</td>
                    <td>{{if .Disabled}}<span class="badge off">禁用</span>{{else}}<span class="badge ok">启用</span>{{end}}</td>
                    <td class="actions">
                      <form class="inline" method="post" action="/admin/accounts/reset-token">
                        <input type="hidden" name="account_id" value="{{.ID}}">
                        <button class="secondary" type="submit">重置 token</button>
                      </form>
                      <form class="inline" method="post" action="/admin/accounts/toggle">
                        <input type="hidden" name="account_id" value="{{.ID}}">
                        {{if .Disabled}}
                        <input type="hidden" name="disabled" value="false"><button type="submit">启用</button>
                        {{else}}
                        <input type="hidden" name="disabled" value="true"><button class="danger" type="submit">禁用</button>
                        {{end}}
                      </form>
                    </td>
                  </tr>
                  {{end}}
                </tbody>
              </table>
            </section>
            <aside>
              <section class="panel">
                <h2>创建账号</h2>
                <form method="post" action="/admin/accounts/create">
                  <div class="form-grid">
                    <div><label>用户名</label><input name="username" required></div>
                    <div><label>显示名</label><input name="display_name"></div>
                  </div>
                  <label>邮箱</label><input name="email" type="email">
                  <label>初始密码</label><input name="password" type="password" required>
                  <label><input style="width:auto" type="checkbox" name="admin"> 管理员</label>
                  <button type="submit">创建账号</button>
                </form>
              </section>
            </aside>
          </div>
        </section>
        {{end}}

        <section class="view" data-view="security">
          <section class="panel compact">
            <h2>安全设置</h2>
            <p class="muted">登录密码只用于管理后台。客户端同步请使用账号同步 token。</p>
            <form method="post" action="/admin/accounts/change-password">
              <label>当前密码</label><input name="current_password" type="password" autocomplete="current-password" required>
              <label>新密码</label><input name="new_password" type="password" autocomplete="new-password" required>
              <label>确认新密码</label><input name="confirm_password" type="password" autocomplete="new-password" required>
              <button type="submit">修改密码</button>
            </form>
          </section>
          <section class="panel compact">
            <h2>同步 token</h2>
            <p class="muted">重置后旧 token 会立即失效。新 token 只显示一次。</p>
            <form method="post" action="/admin/accounts/reset-token">
              <input type="hidden" name="account_id" value="{{.Account.ID}}">
              <button type="submit">重置我的同步 token</button>
            </form>
          </section>
        </section>
      </main>
    </div>
  </div>
  <script>
    (function(){
      var shell = document.querySelector(".shell");
      var buttons = Array.prototype.slice.call(document.querySelectorAll("[data-view-target]"));
      var views = Array.prototype.slice.call(document.querySelectorAll("[data-view]"));
      var menuToggle = document.querySelector("[data-menu-toggle]");
      var menuClose = document.querySelector("[data-menu-close]");
      function closeMenu(){ shell.classList.remove("menu-open"); }
      function show(name){
        views.forEach(function(view){ view.classList.toggle("active", view.getAttribute("data-view") === name); });
        buttons.forEach(function(button){ button.classList.toggle("active", button.getAttribute("data-view-target") === name); });
        closeMenu();
        if (location.hash !== "#" + name) history.replaceState(null, "", "#" + name);
      }
      if (menuToggle) menuToggle.addEventListener("click", function(){ shell.classList.toggle("menu-open"); });
      if (menuClose) menuClose.addEventListener("click", closeMenu);
      buttons.forEach(function(button){ button.addEventListener("click", function(){ show(button.getAttribute("data-view-target")); }); });
      var initial = (location.hash || "#{{.InitialView}}").slice(1);
      if (!document.querySelector('[data-view="' + initial + '"]')) initial = "overview";
      show(initial);
    })();
  </script>
</body>
</html>`
