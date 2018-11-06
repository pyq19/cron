package master

import (
	"encoding/json"
	"github.com/yenkeia/cron/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的 HTTP 接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 全局单例
	G_apiServer *ApiServer
)

// 保存任务接口
// post job={"name": "job1", "command": "echo hello", "cronExpr": "* * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
	)
	// 解析 post 表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	// 取表单中的 job 字段
	postJob = req.PostForm.Get("job")
	// 反序列化 job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	// 保存 job，把 job 传到 jobMgr，再由 jobMgr 传到 etcd

ERR:
}

// 初始化服务
func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)

	// 启动 TCP 监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	// 创建 HTTP 服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动了服务端
	go httpServer.Serve(listener)

	return
}
