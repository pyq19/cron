package main

import (
	"fmt"
	"github.com/yenkeia/cron/master"
	"runtime"
)

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)
	// 初始化线程
	initEnv()

	// 启动 Api HTTP 服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	// 正常退出
	return

ERR:
	fmt.Println(err)
}
