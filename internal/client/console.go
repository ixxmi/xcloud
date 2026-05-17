package client

import (
	"context"
	"errors"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"xcloud/internal/syncmodel"
)

type ConsoleConfig struct {
	Root         string
	ServerURL    string
	ListenAddr   string
	ConfigPath   string
	StatePath    string
	SpaceID      string
	DeviceID     string
	Interval     time.Duration
	ChunkSize    int
	DeleteRemote bool
	Log          *slog.Logger
}

type Console struct {
	cfg         ConsoleConfig
	log         *slog.Logger
	mu          sync.Mutex
	local       LocalConfig
	running     bool
	lastError   string
	lastStarted string
	cancel      context.CancelFunc
	watchCancel context.CancelFunc
}

func NewConsole(cfg ConsoleConfig) (*Console, error) {
	if cfg.ServerURL == "" {
		cfg.ServerURL = "http://127.0.0.1:8080"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:18080"
	}
	if cfg.SpaceID == "" {
		cfg.SpaceID = "default"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = syncmodel.DefaultChunkSize
	}
	if cfg.ConfigPath == "" {
		cfg.ConfigPath = DefaultLocalConfigPath()
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	local, err := LoadLocalConfig(cfg.ConfigPath)
	if err != nil {
		return nil, err
	}
	if local.ServerURL == "" {
		local.ServerURL = cfg.ServerURL
	}
	if local.SpaceID == "" {
		local.SpaceID = cfg.SpaceID
	}
	if local.StorageRoot == "" {
		local.StorageRoot = defaultStorageRoot(cfg.Root)
	}
	effectiveDeviceID := strings.TrimSpace(cfg.DeviceID)
	if effectiveDeviceID == "" {
		effectiveDeviceID = local.DeviceID
	}
	cfg.DeviceID = defaultDeviceID(effectiveDeviceID, local.StorageRoot, local.ServerURL)
	local.DeviceID = cfg.DeviceID
	local.SyncSettings = syncmodel.NormalizeSyncSettings(local.SyncSettings)
	local.DeleteRemote = local.DeleteRemote || cfg.DeleteRemote
	return &Console{cfg: cfg, log: cfg.Log, local: local}, nil
}

func (c *Console) Run(ctx context.Context) error {
	c.startWatcher(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", c.handleIndex)
	mux.HandleFunc("POST /login", c.handleLogin)
	mux.HandleFunc("POST /sync/start", c.handleStart)
	mux.HandleFunc("POST /sync/stop", c.handleStop)
	mux.HandleFunc("POST /logout", c.handleLogout)
	srv := &http.Server{
		Addr:              c.cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", c.cfg.ListenAddr)
	if err != nil {
		return err
	}
	c.log.Info("xcloud client console listening", "addr", c.cfg.ListenAddr, "server", c.local.ServerURL)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		c.stopWatcher()
		c.stopSync()
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		c.stopWatcher()
		c.stopSync()
		return err
	}
}

func (c *Console) handleIndex(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	data := c.viewDataLocked("")
	c.mu.Unlock()
	renderClientConsole(w, data)
}

func (c *Console) handleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		c.renderWithMessage(w, "error", err.Error())
		return
	}
	serverURL := strings.TrimRight(strings.TrimSpace(r.Form.Get("server_url")), "/")
	if serverURL == "" {
		serverURL = c.cfg.ServerURL
	}
	host, _ := os.Hostname()
	api := NewAPI(serverURL, "", c.cfg.SpaceID, c.cfg.DeviceID, "")
	resp, err := api.ClientLogin(syncmodel.ClientLoginRequest{
		Identifier:  r.Form.Get("identifier"),
		Password:    r.Form.Get("password"),
		DeviceID:    c.cfg.DeviceID,
		Hostname:    host,
		StorageRoot: c.local.StorageRoot,
	})
	if err != nil {
		c.renderWithMessage(w, "error", "云端账号登录失败："+err.Error())
		return
	}
	c.mu.Lock()
	c.local.ServerURL = serverURL
	c.local.Token = resp.Token
	if resp.StorageRoot != "" {
		c.local.StorageRoot = resp.StorageRoot
	}
	if err := os.MkdirAll(c.local.StorageRoot, 0o755); err != nil {
		c.mu.Unlock()
		c.renderWithMessage(w, "error", "创建本机保存目录失败："+err.Error())
		return
	}
	c.local.SpaceID = resp.SpaceID
	c.local.DeviceID = c.cfg.DeviceID
	c.local.Username = resp.Account.Username
	c.local.DisplayName = resp.Account.DisplayName
	c.local.SyncEnabled = resp.SyncEnabled
	c.local.SyncSettings = syncmodel.NormalizeSyncSettings(resp.Settings)
	c.local.DeleteRemote = c.cfg.DeleteRemote
	err = SaveLocalConfig(c.cfg.ConfigPath, c.local)
	if err == nil && c.local.SyncEnabled {
		c.startSyncLocked()
	}
	c.mu.Unlock()
	if err != nil {
		c.renderWithMessage(w, "error", err.Error())
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

func (c *Console) handleStart(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	if c.local.Token == "" {
		c.mu.Unlock()
		c.renderWithMessage(w, "error", "请先登录云端账号")
		return
	}
	api := NewAPI(c.local.ServerURL, c.local.Token, c.local.SpaceID, c.local.DeviceID, "")
	status, err := api.SetClientSyncEnabled(true)
	if err != nil {
		c.mu.Unlock()
		c.renderWithMessage(w, "error", "开启同步失败："+err.Error())
		return
	}
	c.local.SyncEnabled = status.SyncEnabled
	c.local.SyncSettings = syncmodel.NormalizeSyncSettings(status.Settings)
	if status.StorageRoot != "" {
		c.local.StorageRoot = status.StorageRoot
	}
	if c.local.StorageRoot != "" {
		if err := os.MkdirAll(c.local.StorageRoot, 0o755); err != nil {
			c.mu.Unlock()
			c.renderWithMessage(w, "error", "创建本机保存目录失败："+err.Error())
			return
		}
	}
	if err := SaveLocalConfig(c.cfg.ConfigPath, c.local); err != nil {
		c.mu.Unlock()
		c.renderWithMessage(w, "error", err.Error())
		return
	}
	if c.local.SyncEnabled {
		c.startSyncLocked()
	}
	c.mu.Unlock()
	http.Redirect(w, r, "/", http.StatusFound)
}

func (c *Console) handleStop(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	if c.local.Token == "" {
		c.mu.Unlock()
		c.renderWithMessage(w, "error", "请先登录云端账号")
		return
	}
	api := NewAPI(c.local.ServerURL, c.local.Token, c.local.SpaceID, c.local.DeviceID, "")
	status, err := api.SetClientSyncEnabled(false)
	if err == nil {
		c.local.SyncEnabled = status.SyncEnabled
		err = SaveLocalConfig(c.cfg.ConfigPath, c.local)
	}
	c.mu.Unlock()
	if err != nil {
		c.renderWithMessage(w, "error", "暂停同步失败："+err.Error())
		return
	}
	c.stopSync()
	http.Redirect(w, r, "/", http.StatusFound)
}

func (c *Console) handleLogout(w http.ResponseWriter, r *http.Request) {
	c.stopSync()
	c.mu.Lock()
	c.local.Token = ""
	c.local.Username = ""
	c.local.DisplayName = ""
	c.local.SyncEnabled = false
	err := SaveLocalConfig(c.cfg.ConfigPath, c.local)
	c.mu.Unlock()
	if err != nil {
		c.renderWithMessage(w, "error", err.Error())
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

func (c *Console) startWatcher(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	c.watchCancel = cancel
	c.mu.Unlock()
	go c.watchCloudSync(ctx)
}

func (c *Console) stopWatcher() {
	c.mu.Lock()
	cancel := c.watchCancel
	c.watchCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Console) watchCloudSync(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		c.refreshCloudSync(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Console) refreshCloudSync(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	c.mu.Lock()
	token := c.local.Token
	serverURL := c.local.ServerURL
	spaceID := c.local.SpaceID
	deviceID := c.local.DeviceID
	c.mu.Unlock()
	if token == "" || serverURL == "" {
		return
	}
	status, err := NewAPI(serverURL, token, spaceID, deviceID, "").ClientStatus()
	if err != nil {
		c.mu.Lock()
		c.lastError = "同步状态检查失败：" + err.Error()
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	c.local.SyncEnabled = status.SyncEnabled
	c.local.SyncSettings = syncmodel.NormalizeSyncSettings(status.Settings)
	if status.StorageRoot != "" {
		c.local.StorageRoot = status.StorageRoot
	}
	if c.local.StorageRoot != "" {
		if err := os.MkdirAll(c.local.StorageRoot, 0o755); err != nil {
			c.lastError = "创建本机保存目录失败：" + err.Error()
			c.mu.Unlock()
			return
		}
	}
	c.local.SpaceID = status.SpaceID
	c.local.Username = status.Account.Username
	c.local.DisplayName = status.Account.DisplayName
	_ = SaveLocalConfig(c.cfg.ConfigPath, c.local)
	if c.local.SyncEnabled {
		c.startSyncLocked()
		c.mu.Unlock()
		return
	}
	cancel := c.cancel
	c.cancel = nil
	c.running = false
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Console) startSyncLocked() {
	if c.running || c.local.Token == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.running = true
	c.lastError = ""
	c.lastStarted = time.Now().Format("2006-01-02 15:04:05")
	cfg := Config{
		ServerURL:    c.local.ServerURL,
		Token:        c.local.Token,
		StorageRoot:  c.local.StorageRoot,
		SpaceID:      c.local.SpaceID,
		DeviceID:     c.local.DeviceID,
		StatePath:    c.cfg.StatePath,
		Interval:     c.cfg.Interval,
		Settings:     syncmodel.NormalizeSyncSettings(c.local.SyncSettings),
		ChunkSize:    c.cfg.ChunkSize,
		DeleteRemote: c.local.DeleteRemote,
		Log:          c.log,
	}
	go func() {
		var err error
		var supervisor *Supervisor
		supervisor, err = NewSupervisor(cfg)
		if err == nil {
			err = supervisor.Run(ctx)
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			c.setSyncStopped(err)
			return
		}
		c.setSyncStopped(nil)
	}()
}

func (c *Console) stopSync() {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.running = false
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Console) setSyncStopped(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	c.cancel = nil
	if err != nil {
		c.lastError = err.Error()
	}
}

func (c *Console) renderWithMessage(w http.ResponseWriter, kind, text string) {
	c.mu.Lock()
	data := c.viewDataLocked(text)
	data.MessageKind = kind
	c.mu.Unlock()
	renderClientConsole(w, data)
}

func (c *Console) viewDataLocked(message string) clientConsoleData {
	return clientConsoleData{
		ServerURL:    c.local.ServerURL,
		Username:     c.local.Username,
		DisplayName:  c.local.DisplayName,
		LoggedIn:     c.local.Token != "",
		SyncEnabled:  c.local.SyncEnabled,
		Running:      c.running,
		DeviceID:     c.local.DeviceID,
		SpaceID:      c.local.SpaceID,
		ConfigPath:   c.cfg.ConfigPath,
		StorageRoot:  c.local.StorageRoot,
		LastStarted:  c.lastStarted,
		LastError:    c.lastError,
		Message:      message,
		MessageKind:  "success",
		DeleteRemote: c.local.DeleteRemote,
	}
}

type clientConsoleData struct {
	ServerURL    string
	Username     string
	DisplayName  string
	LoggedIn     bool
	SyncEnabled  bool
	Running      bool
	DeviceID     string
	SpaceID      string
	ConfigPath   string
	StorageRoot  string
	LastStarted  string
	LastError    string
	Message      string
	MessageKind  string
	DeleteRemote bool
}

func defaultStorageRoot(root string) string {
	if strings.TrimSpace(root) != "" {
		if abs, err := filepath.Abs(root); err == nil {
			return abs
		}
		return root
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "xcloud"
	}
	return filepath.Join(cwd, "xcloud")
}

func renderClientConsole(w http.ResponseWriter, data clientConsoleData) {
	page := template.Must(template.New("client-console").Parse(clientConsoleHTML))
	_ = page.Execute(w, data)
}

const clientConsoleHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>xcloud 客户端</title>
  <style>
    :root{--ink:#000;--canvas:#fff;--hair:#e6e6e6;--soft:#f7f7f5;--lime:#dceeb1;--lilac:#c5b0f4;--cream:#f4ecd6;--pink:#efd4d4;--mint:#c8e6cd}
    *{box-sizing:border-box}body{margin:0;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;background:var(--canvas);color:var(--ink)}
    .page{min-height:100vh;display:grid;grid-template-columns:minmax(0,1fr) 420px}.hero{position:relative;padding:34px 48px;border-right:1px solid var(--hair);overflow:hidden}.hero:before{content:"";position:absolute;inset:108px 46px auto auto;width:min(620px,58vw);height:390px;background:var(--lime);border-radius:24px;transform:rotate(-2deg)}.brand,.copy,.steps{position:relative;z-index:1}.brand{display:flex;align-items:center;gap:12px;font-weight:800}.logo{width:36px;height:36px;border-radius:9999px;background:var(--ink);color:var(--canvas);display:flex;align-items:center;justify-content:center}.copy{max-width:760px;margin-top:92px}.copy h1{font-size:64px;line-height:1;margin:0 0 20px;font-weight:340;letter-spacing:0}.copy p{font-size:20px;line-height:1.4;max-width:620px}.steps{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:80px}.step{border:1px solid var(--ink);border-radius:24px;padding:18px;background:var(--canvas)}.step:nth-child(2){background:var(--mint)}.step:nth-child(3){background:var(--pink)}.step b{display:block;font-size:22px;margin-bottom:6px}.panel{display:flex;align-items:center;justify-content:center;background:var(--cream);padding:28px}.card{width:100%;max-width:372px;border:1px solid var(--ink);border-radius:24px;background:var(--canvas);padding:28px;box-shadow:8px 8px 0 var(--ink)}h2{margin:0 0 8px;font-size:30px;font-weight:540}.muted{font-size:14px;line-height:1.55}.msg{border:1px solid var(--ink);border-radius:8px;padding:12px;margin:14px 0}.msg.error{background:var(--pink)}.msg.success{background:var(--mint)}label{display:block;margin:14px 0 6px;font-size:12px;text-transform:uppercase;letter-spacing:.6px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}input{width:100%;height:46px;border:1px solid var(--hair);border-radius:8px;padding:0 12px;font-size:16px}input:focus{outline:none;border-color:var(--ink);box-shadow:0 0 0 3px var(--lime)}button{width:100%;height:46px;border:0;border-radius:9999px;background:var(--ink);color:var(--canvas);font-size:16px;font-weight:700;cursor:pointer;margin-top:16px}.secondary{background:var(--canvas);color:var(--ink);border:1px solid var(--ink)}.danger{background:#ff3d8b}.status{border:1px solid var(--ink);border-radius:24px;background:var(--lime);padding:18px;margin:18px 0}.rows{display:grid;gap:9px;margin-top:14px}.row{display:flex;justify-content:space-between;gap:12px;border-bottom:1px solid var(--hair);padding-bottom:8px;font-size:14px}.row span:last-child{text-align:right;word-break:break-all}.badge{display:inline-flex;border:1px solid var(--ink);border-radius:9999px;padding:4px 9px;background:var(--canvas);font-size:12px;font-weight:700}@media(max-width:920px){.page{grid-template-columns:1fr}.hero{display:none}.panel{min-height:100vh}.card{box-shadow:5px 5px 0 var(--ink)}}@media(max-width:560px){.panel{padding:18px}.card{padding:22px}}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="brand"><div class="logo">X</div><span>xcloud 客户端</span></div>
      <div class="copy">
        <h1>登录云端账号后开启同步</h1>
        <p>首次启动不需要手工复制 token。客户端会保存本机凭证；开启同步后，xcloud 目录下的所有 Space 文件会和同账号设备保持一致。</p>
      </div>
      <div class="steps">
        <div class="step"><b>1</b><span>登录云端账号</span></div>
        <div class="step"><b>2</b><span>点击开启同步</span></div>
        <div class="step"><b>3</b><span>把文件放入 xcloud</span></div>
      </div>
    </section>
    <section class="panel">
      <div class="card">
        {{if .LoggedIn}}
        <h2>客户端已绑定</h2>
        <p class="muted">当前客户端只同步保存根目录下的 Space 子目录。默认 Space 路径是 xcloud/default。</p>
        {{else}}
        <h2>绑定云端账号</h2>
        <p class="muted">输入云端管理后台账号，客户端会换取本机专用同步凭证。</p>
        {{end}}
        {{if .Message}}<div class="msg {{.MessageKind}}">{{.Message}}</div>{{end}}
        {{if .LoggedIn}}
        <div class="status">
          <span class="badge">{{if .Running}}同步运行中{{else if .SyncEnabled}}等待启动{{else}}同步已暂停{{end}}</span>
          <div class="rows">
            <div class="row"><span>账号</span><span>{{if .DisplayName}}{{.DisplayName}}{{else}}{{.Username}}{{end}}</span></div>
            <div class="row"><span>云端</span><span>{{.ServerURL}}</span></div>
            <div class="row"><span>设备</span><span>{{.DeviceID}}</span></div>
            <div class="row"><span>Space</span><span>{{.SpaceID}}</span></div>
            <div class="row"><span>保存根目录</span><span>{{.StorageRoot}}</span></div>
            <div class="row"><span>同步范围</span><span>{{.StorageRoot}}/&lt;space-id&gt;</span></div>
            <div class="row"><span>配置</span><span>{{.ConfigPath}}</span></div>
            {{if .LastStarted}}<div class="row"><span>启动时间</span><span>{{.LastStarted}}</span></div>{{end}}
            {{if .LastError}}<div class="row"><span>错误</span><span>{{.LastError}}</span></div>{{end}}
          </div>
        </div>
        {{if .SyncEnabled}}
        <form method="post" action="/sync/stop"><button class="secondary" type="submit">暂停此账号同步</button></form>
        {{else}}
        <form method="post" action="/sync/start"><button type="submit">开启此账号同步</button></form>
        {{end}}
        <form method="post" action="/logout"><button class="danger" type="submit">解除绑定</button></form>
        {{else}}
        <form method="post" action="/login">
          <label>云端地址</label>
          <input name="server_url" value="{{.ServerURL}}" placeholder="http://127.0.0.1:8080" required>
          <label>账号或邮箱</label>
          <input name="identifier" autocomplete="username" required>
          <label>密码</label>
          <input name="password" type="password" autocomplete="current-password" required>
          <button type="submit">登录并绑定</button>
        </form>
        {{end}}
      </div>
    </section>
  </div>
</body>
</html>`
