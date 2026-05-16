package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"xcloud/internal/fileutil"
	"xcloud/internal/syncmodel"
)

const sessionCookie = "xcloud_session"

type Server struct {
	store    *Store
	log      *slog.Logger
	sessions map[string]string
	mu       sync.Mutex
}

type syncContext struct {
	account  syncmodel.Account
	spaceID  string
	deviceID string
	rootPath string
}

func New(store *Store, _ string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		store:    store,
		log:      log,
		sessions: map[string]string{},
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
	mux.HandleFunc("POST /admin/folders/select", s.requireLogin(s.adminSelectFolder))
	mux.HandleFunc("POST /admin/folders/disable", s.requireLogin(s.adminDisableFolder))

	mux.HandleFunc("POST /v1/folders/report", s.syncAuth(s.reportFolder))
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
		if r.URL.Path != "/v1/folders/report" {
			if deviceID == "" || rootPath == "" {
				writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "client folder has not been reported"})
				return
			}
			if _, ok := s.store.GetSpace(account.ID, spaceID); !ok {
				writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "sync space not found"})
				return
			}
			if !s.store.FolderSelected(account.ID, deviceID, rootPath, spaceID) {
				writeJSON(w, http.StatusForbidden, syncmodel.ErrorResponse{Error: "client folder is not selected for sync"})
				return
			}
		}
		next(w, r, syncContext{account: *account, spaceID: spaceID, deviceID: deviceID, rootPath: rootPath})
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
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: fmt.Sprintf("Space %s 已创建。客户端可用 -space %s 作为上报建议，最终由网关选择生效。", space.Name, space.ID)})
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
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限修改其他账号 Space"})
		return
	}
	if err := s.store.SetSpaceActive(accountID, r.Form.Get("space_id"), r.Form.Get("active") == "true"); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "Space 状态已更新"})
}

func (s *Server) adminSelectFolder(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限选择其他账号的客户端目录"})
		return
	}
	if err := s.store.SelectFolder(accountID, r.Form.Get("folder_id"), r.Form.Get("space_id")); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "客户端目录已选择，客户端下一轮同步会开始工作"})
}

func (s *Server) adminDisableFolder(w http.ResponseWriter, r *http.Request, account syncmodel.Account) {
	if err := r.ParseForm(); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	accountID := r.Form.Get("account_id")
	if accountID == "" {
		accountID = account.ID
	}
	if accountID != account.ID && !account.IsAdmin {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: "没有权限停用其他账号的客户端目录"})
		return
	}
	if err := s.store.DisableFolder(accountID, r.Form.Get("folder_id")); err != nil {
		s.renderDashboard(w, account, flashMessage{Kind: "error", Text: err.Error()})
		return
	}
	s.renderDashboard(w, account, flashMessage{Kind: "success", Text: "客户端目录已停用"})
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

func (s *Server) renderAuth(w http.ResponseWriter, data authPageData) {
	if data.Mode == "" {
		data.Mode = "login"
	}
	page := template.Must(template.New("auth").Parse(authHTML))
	_ = page.Execute(w, data)
}

func (s *Server) renderDashboard(w http.ResponseWriter, account syncmodel.Account, flash flashMessage) {
	accounts := []syncmodel.Account{account}
	if account.IsAdmin {
		accounts = s.store.ListAccounts()
	}
	type folderView struct {
		Folder syncmodel.ClientFolder
		Spaces []syncmodel.SpaceSummary
	}
	type spaceGroup struct {
		Account syncmodel.Account
		Spaces  []syncmodel.SpaceSummary
		Folders []folderView
	}
	groups := make([]spaceGroup, 0, len(accounts))
	totalSpaces := 0
	totalFiles := 0
	totalDeleted := 0
	activeSpaces := 0
	for _, item := range accounts {
		spaces := s.store.ListSpaces(item.ID)
		folders := s.store.ListFolders(item.ID)
		folderViews := make([]folderView, 0, len(folders))
		for _, folder := range folders {
			folderViews = append(folderViews, folderView{Folder: folder, Spaces: spaces})
		}
		for _, summary := range spaces {
			totalSpaces++
			totalFiles += summary.FileCount
			totalDeleted += summary.Deleted
			if summary.Space.Active {
				activeSpaces++
			}
		}
		groups = append(groups, spaceGroup{
			Account: item,
			Spaces:  spaces,
			Folders: folderViews,
		})
	}
	page := template.Must(template.New("dashboard").Parse(dashboardHTML))
	_ = page.Execute(w, map[string]any{
		"Account":       account,
		"Accounts":      accounts,
		"Groups":        groups,
		"Flash":         flash,
		"TotalAccounts": len(accounts),
		"TotalSpaces":   totalSpaces,
		"ActiveSpaces":  activeSpaces,
		"TotalFiles":    totalFiles,
		"TotalDeleted":  totalDeleted,
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

const authHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>xcloud 管理后台</title>
  <style>
    :root{color-scheme:dark}
    *{box-sizing:border-box}body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;background:#09111f;color:#f8fafc}
    .page{min-height:100vh;display:grid;grid-template-columns:minmax(0,1fr) 440px;overflow:hidden;background:#08111f}
    .visual{position:relative;padding:56px;display:flex;flex-direction:column;justify-content:space-between;background:linear-gradient(135deg,#0f172a 0%,#12304d 58%,#0f3f3c 100%)}
    .visual:before{content:"";position:absolute;inset:0;background-image:linear-gradient(rgba(255,255,255,.07) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.07) 1px,transparent 1px);background-size:42px 42px;mask-image:linear-gradient(90deg,#000,transparent 85%)}
    .brand,.hero,.metrics{position:relative}.brand{display:flex;align-items:center;gap:12px;font-weight:800;font-size:21px}.logo{width:36px;height:36px;border-radius:8px;background:#2dd4bf;color:#06111f;display:flex;align-items:center;justify-content:center;font-weight:900}
    .hero{max-width:680px}.hero h1{font-size:48px;line-height:1.05;margin:0 0 18px;letter-spacing:0}.hero p{font-size:17px;line-height:1.7;color:#cbd5e1;max-width:620px;margin:0}
    .metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;max-width:680px}.metric{border:1px solid rgba(255,255,255,.14);background:rgba(15,23,42,.52);border-radius:8px;padding:16px}.metric strong{display:block;font-size:24px}.metric span{font-size:12px;color:#cbd5e1}
    .panel{background:linear-gradient(180deg,#0b1220,#101827);display:flex;align-items:center;justify-content:center;padding:28px;border-left:1px solid rgba(255,255,255,.08)}
    .card{width:100%;max-width:386px;background:rgba(15,23,42,.92);border:1px solid rgba(148,163,184,.24);border-radius:8px;padding:30px;box-shadow:0 24px 80px rgba(0,0,0,.38)}
    .tabs{display:grid;grid-template-columns:1fr 1fr;background:#111827;border:1px solid #263244;border-radius:8px;padding:4px;margin-bottom:24px}.tabs a{height:36px;display:flex;align-items:center;justify-content:center;border-radius:6px;text-decoration:none;color:#94a3b8;font-weight:700;font-size:14px}.tabs a.active{background:#2563eb;color:#fff}
    h2{margin:0 0 8px;font-size:26px;letter-spacing:0}.sub{margin:0 0 22px;color:#94a3b8;font-size:14px;line-height:1.6}.msg{border-radius:8px;padding:11px 12px;margin-bottom:14px;font-size:13px;line-height:1.5}.msg.error{background:#451a1a;color:#fecaca;border:1px solid #7f1d1d}.msg.success{background:#052e2b;color:#99f6e4;border:1px solid #0f766e}
    label{display:block;font-size:13px;font-weight:700;margin:14px 0 7px;color:#dbeafe}.field{position:relative}.field input{width:100%;height:42px;border:1px solid #263244;background:#111827;color:#f8fafc;border-radius:8px;padding:0 58px 0 42px;font-size:14px;outline:none}.field input:focus{border-color:#60a5fa;box-shadow:0 0 0 3px rgba(37,99,235,.22)}.ico{position:absolute;left:12px;top:50%;transform:translateY(-50%);color:#64748b;font-size:12px;font-weight:800}.toggle{position:absolute;right:8px;top:50%;transform:translateY(-50%);height:28px;border:0;background:transparent;color:#94a3b8;cursor:pointer;font-size:12px}
    .row{display:flex;justify-content:space-between;align-items:center;margin:16px 0 20px;color:#94a3b8;font-size:13px}.row label{margin:0;font-weight:500;color:#94a3b8}.row input{vertical-align:middle;margin-right:6px}.link{color:#67e8f9;text-decoration:none;border:0;background:transparent;cursor:pointer;font:inherit}
    .submit{width:100%;height:42px;border:0;border-radius:8px;background:#2563eb;color:#fff;font-weight:800;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:8px}.submit:hover{background:#1d4ed8}.foot{margin:20px 0 0;text-align:center;color:#94a3b8;font-size:13px}
    .hint{margin-top:18px;color:#94a3b8;font-size:12px;line-height:1.6}.hidden{display:none}
    @media(max-width:920px){.page{grid-template-columns:1fr}.visual{display:none}.panel{min-height:100vh;border:0}}
  </style>
</head>
<body>
  <div class="page">
    <section class="visual">
      <div class="brand"><div class="logo">X</div><span>xcloud 控制台</span></div>
      <div class="hero">
        <h1>账号隔离的文件同步中心</h1>
        <p>管理账号、同步 token、客户端目录和 Space。每个账号的数据只在自己的 Space 内流转，服务端负责版本协调、冲突保留和 chunk 校验。</p>
      </div>
      <div class="metrics">
        <div class="metric"><strong>SHA-256</strong><span>端到端完整性校验</span></div>
        <div class="metric"><strong>Space</strong><span>账号内多目录隔离</span></div>
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
        <p class="sub">使用用户名或邮箱登录管理你的客户端目录和 Space。</p>
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
    *{box-sizing:border-box}body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;background:#f6f8fb;color:#172033}
    .shell{min-height:100vh;display:flex}.side{width:252px;background:#0f172a;color:#cbd5e1;display:flex;flex-direction:column;position:fixed;inset:0 auto 0 0}.brand{height:64px;display:flex;align-items:center;gap:12px;padding:0 20px;border-bottom:1px solid #1e293b}.logo{width:34px;height:34px;border-radius:8px;background:#2dd4bf;color:#06111f;display:flex;align-items:center;justify-content:center;font-weight:900}.brand b{color:#fff;font-size:18px}.brand span{color:#38bdf8}
    .nav{padding:18px 14px;flex:1}.nav p{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#64748b;margin:10px 8px}.nav button{width:100%;height:40px;display:flex;align-items:center;gap:10px;padding:0 10px;border-radius:8px;color:#cbd5e1;text-decoration:none;font-size:14px;margin:4px 0;background:transparent;border:0;font-weight:700;cursor:pointer;text-align:left}.nav button.active{background:#1d4ed8;color:#fff}.nav button:not(.active):hover{background:#1e293b;color:#fff}.logout{padding:16px;border-top:1px solid #1e293b}.logout button{width:100%;height:38px;border:0;border-radius:8px;background:#1e293b;color:#e2e8f0;font-weight:700;cursor:pointer}
    .main{margin-left:252px;min-width:0;flex:1}.top{height:64px;background:#fff;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between;padding:0 26px;position:sticky;top:0;z-index:5}.search{height:38px;width:min(360px,48vw);display:flex;align-items:center;gap:8px;background:#f1f5f9;border:1px solid #e2e8f0;border-radius:8px;padding:0 12px;color:#64748b}.search input{border:0;background:transparent;outline:none;width:100%;font-size:14px}.user{display:flex;align-items:center;gap:12px}.avatar{width:34px;height:34px;border-radius:50%;background:#2563eb;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:800}.content{padding:28px;max-width:1280px;margin:0 auto}.hero{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:22px}.hero h1{font-size:26px;margin:0 0 6px;letter-spacing:0}.muted{color:#667085;font-size:13px;line-height:1.6}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px}.stat{background:#fff;border:1px solid #e6eaf0;border-radius:8px;padding:18px}.stat .label{font-size:13px;color:#667085;margin-bottom:8px}.stat strong{font-size:28px}.stat .hint{font-size:12px;color:#0f766e;margin-top:10px}
    .two{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(320px,.8fr);gap:18px;margin-top:18px}.panel{background:#fff;border:1px solid #e6eaf0;border-radius:8px;padding:18px;margin-bottom:18px}.panel h2{font-size:17px;margin:0 0 14px}.panel h3{font-size:15px;margin:20px 0 10px}.flash{border-radius:8px;padding:12px 14px;margin-bottom:18px;word-break:break-all;font-size:14px}.flash.success{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46}.flash.error{background:#fef2f2;border:1px solid #fecaca;color:#991b1b}
    table{width:100%;border-collapse:collapse;font-size:14px}th,td{border-bottom:1px solid #eef2f7;text-align:left;padding:10px 8px;vertical-align:top}th{font-size:12px;color:#667085;text-transform:uppercase;letter-spacing:.04em}code,.cmd{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.cmd{background:#f1f5f9;border:1px solid #e2e8f0;border-radius:8px;padding:10px;word-break:break-all;color:#334155}
    label{display:block;font-size:13px;font-weight:700;margin:12px 0 6px}input,textarea,select{box-sizing:border-box;width:100%;border:1px solid #cbd5e1;border-radius:8px;padding:9px 10px;font-size:14px;background:#fff;color:#172033}textarea{min-height:72px;resize:vertical}.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.inline{display:inline}
    button{border:0;border-radius:8px;background:#2563eb;color:#fff;font-weight:800;padding:9px 12px;cursor:pointer}.secondary{background:#eef2f7;color:#172033}.danger{background:#dc2626}.badge{display:inline-flex;align-items:center;height:24px;padding:0 8px;border-radius:999px;font-size:12px;font-weight:700;background:#eef2f7;color:#334155}.ok{background:#dcfce7;color:#166534}.off{background:#fee2e2;color:#991b1b}
    .view{display:none}.view.active{display:block}.overview-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.compact{max-width:760px}.section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:14px}.small-select{min-width:130px}.path{max-width:360px;word-break:break-all}
    @media(max-width:980px){.side{display:none}.main{margin-left:0}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.two{grid-template-columns:1fr}.top{padding:0 16px}.content{padding:18px}.hero{flex-direction:column}}@media(max-width:560px){.grid,.form-grid{grid-template-columns:1fr}.search{display:none}}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="side">
      <div class="brand"><div class="logo">X</div><b>xcloud<span>Admin</span></b></div>
      <nav class="nav">
        <p>主菜单</p>
        <button class="active" type="button" data-view-target="overview">▣ 概览</button>
        <button type="button" data-view-target="spaces">▤ 目录与 Space</button>
        {{if .Account.IsAdmin}}<button type="button" data-view-target="accounts">◎ 账号管理</button>{{end}}
        <button type="button" data-view-target="security">⚙ 安全设置</button>
      </nav>
      <div class="logout"><form method="post" action="/admin/logout"><button type="submit">退出登录</button></form></div>
    </aside>
    <div class="main">
      <header class="top">
        <div class="search">⌕ <input placeholder="搜索账号、客户端目录或 Space ID"></div>
        <div class="user"><span class="muted">{{.Account.DisplayName}} / {{.Account.Username}}{{if .Account.IsAdmin}} / 管理员{{end}}</span><div class="avatar">{{printf "%.1s" .Account.Username}}</div></div>
      </header>
      <main class="content">
        {{if .Flash.HasText}}<div class="flash {{.Flash.Kind}}">{{.Flash.Text}}</div>{{end}}
        <section class="view active" data-view="overview">
          <div class="hero">
            <div>
              <h1>同步控制台</h1>
              <p class="muted">客户端会上报本地目录到网关。只有在这里选择并分配到 Space 后，同账号同 Space 的目录才会互相同步。</p>
            </div>
            <div class="overview-actions">
              <button type="button" class="secondary" data-view-target="spaces">查看客户端目录</button>
              <form method="post" action="/admin/accounts/reset-token">
                <input type="hidden" name="account_id" value="{{.Account.ID}}">
                <button type="submit">重置我的同步 token</button>
              </form>
            </div>
          </div>
          <div class="grid">
            <div class="stat"><div class="label">账号数</div><strong>{{.TotalAccounts}}</strong><div class="hint">当前可管理范围</div></div>
            <div class="stat"><div class="label">Space</div><strong>{{.TotalSpaces}}</strong><div class="hint">{{.ActiveSpaces}} 个启用中</div></div>
            <div class="stat"><div class="label">有效文件</div><strong>{{.TotalFiles}}</strong><div class="hint">不含删除 tombstone</div></div>
            <div class="stat"><div class="label">删除记录</div><strong>{{.TotalDeleted}}</strong><div class="hint">可用于后续恢复能力</div></div>
          </div>
          <section class="panel" style="margin-top:18px">
            <h2>客户端命令</h2>
            <p class="muted">客户端启动后会先上报本机目录。网关选择目录前，客户端不会上传或下载文件。-space 只是建议分组，实际同步 Space 由这里选择。</p>
            <div class="cmd">./xcloud client -root /path/to/folder -server http://127.0.0.1:8080 -token &lt;账号token&gt; -space default</div>
          </section>
        </section>

        <section class="view" data-view="spaces">
          <div class="two">
            <div>
            {{range .Groups}}
            <section class="panel">
              <div class="section-head">
                <div>
                  <h2>{{.Account.Username}} 的客户端目录</h2>
                  <p class="muted">客户端上报的本地目录会先进入待选择状态。选择到某个 Space 后，同账号同 Space 的目录开始互相同步。</p>
                </div>
              </div>
              <table>
                <thead><tr><th>设备</th><th>本地目录</th><th>建议 Space</th><th>当前 Space</th><th>状态</th><th>最后上报</th><th>操作</th></tr></thead>
                <tbody>
                  {{range .Folders}}
                  <tr>
                    <td><strong>{{.Folder.DeviceID}}</strong><br><span class="muted">{{.Folder.Hostname}}</span></td>
                    <td class="path"><code>{{.Folder.RootPath}}</code></td>
                    <td><code>{{.Folder.SuggestedSpaceID}}</code></td>
                    <td>{{if .Folder.SpaceID}}<code>{{.Folder.SpaceID}}</code>{{else}}<span class="muted">未选择</span>{{end}}</td>
                    <td>{{if eq .Folder.Status "selected"}}<span class="badge ok">已选择</span>{{else if eq .Folder.Status "disabled"}}<span class="badge off">已停用</span>{{else}}<span class="badge">待选择</span>{{end}}</td>
                    <td>{{.Folder.LastSeenAt}}</td>
                    <td>
                      <form class="inline actions" method="post" action="/admin/folders/select">
                        <input type="hidden" name="account_id" value="{{.Folder.AccountID}}">
                        <input type="hidden" name="folder_id" value="{{.Folder.ID}}">
                        <select class="small-select" name="space_id">
                          {{range .Spaces}}<option value="{{.Space.ID}}">{{.Space.Name}}</option>{{end}}
                        </select>
                        <button type="submit">选择</button>
                      </form>
                      <form class="inline" method="post" action="/admin/folders/disable">
                        <input type="hidden" name="account_id" value="{{.Folder.AccountID}}">
                        <input type="hidden" name="folder_id" value="{{.Folder.ID}}">
                        <button class="secondary" type="submit">停用</button>
                      </form>
                    </td>
                  </tr>
                  {{else}}
                  <tr><td colspan="7"><span class="muted">暂无客户端目录上报。启动客户端后会先出现在这里，选择到 Space 后才开始同步。</span></td></tr>
                  {{end}}
                </tbody>
              </table>
            </section>
            {{end}}
            </div>
            <aside>
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
            {{range .Groups}}
            <section class="panel">
              <h2>{{.Account.Username}} 的 Space</h2>
              <table>
                <thead><tr><th>名称</th><th>ID</th><th>目录</th><th>状态</th><th>操作</th></tr></thead>
                <tbody>
                  {{range .Spaces}}
                  <tr>
                    <td>{{.Space.Name}}</td>
                    <td><code>{{.Space.ID}}</code></td>
                    <td>{{.Folders}}</td>
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
                  {{end}}
                </tbody>
              </table>
            </section>
            {{end}}
            </aside>
          </div>
        </section>

        {{if .Account.IsAdmin}}
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
      var buttons = Array.prototype.slice.call(document.querySelectorAll("[data-view-target]"));
      var views = Array.prototype.slice.call(document.querySelectorAll("[data-view]"));
      function show(name){
        views.forEach(function(view){ view.classList.toggle("active", view.getAttribute("data-view") === name); });
        buttons.forEach(function(button){ button.classList.toggle("active", button.getAttribute("data-view-target") === name); });
        if (location.hash !== "#" + name) history.replaceState(null, "", "#" + name);
      }
      buttons.forEach(function(button){ button.addEventListener("click", function(){ show(button.getAttribute("data-view-target")); }); });
      var initial = (location.hash || "#overview").slice(1);
      if (!document.querySelector('[data-view="' + initial + '"]')) initial = "overview";
      show(initial);
    })();
  </script>
</body>
</html>`
