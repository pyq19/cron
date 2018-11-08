package worker

import (
	"context"
	"github.com/yenkeia/cron/common"
	"os/exec"
	"time"
)

// 调度执行器
type Executor struct{}

var G_executor *Executor

// 执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd    *exec.Cmd
			err    error
			output []byte
			result *common.JobExecuteResult
		)
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}
		// 记录任务开始时间
		result.StartTime = time.Now()
		// 执行 shell 命令
		cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", info.Job.Command)
		// 执行并捕获输出
		output, err = cmd.CombinedOutput()
		// 记录任务结束时间
		result.EndTime = time.Now()
		result.Output = output
		result.Err = err
		// 任务执行完成后把执行结果返回给 scheduler, scheduler 会从 executingTable 中删除执行记录
		G_scheduler.PushJobResult(result)
	}()
}

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
