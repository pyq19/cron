package worker

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/yenkeia/cron/common"
	"time"
)

// mongodb 存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var G_logSink *LogSink

func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	var (
		err error
		//result *mongo.InsertManyResult
	)
	if _, err = logSink.logCollection.InsertMany(context.TODO(), batch.Logs); err != nil {
		fmt.Println("InsertMany错误:", err.Error())
	}
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch // 当前的批次
		commitTimmer *time.Timer
		timeoutBatch *common.LogBatch // 超时批次
	)

	for {
		select {
		case log = <-logSink.logChan:
			// 把 log 写到 mongodb 中
			// 这次插入需要等 mongodb 的一次请求往返，耗时因为网络慢花费较长时间
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 让这个批次超时（1秒）时自动提交
				commitTimmer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					// 回调函数是在另一个协程中运行, 需做串行化处理, 即发一个通知给 writeLoop
					// 发送超时通知，不要直接提交 batch
					// TODO !!!
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}
			// 把新的日志追加到批次中去
			logBatch.Logs = append(logBatch.Logs, log)
			// 如果批次满了，就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				// 发送日志
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器，让其重新计时
				commitTimmer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan:
			// 判断过期批次是否仍是当前批次 防止重复提交引发计时器bug
			if timeoutBatch != logBatch {
				// 如果不是当前批次 跳过
				continue
			}
			// 接收到 1s 过期的超时批次, 写入mongodb
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	var (
		client        *mongo.Client
		clientOptions *options.ClientOptions
	)
	clientOptions = options.Client()
	clientOptions.SetAuth(options.Credential{
		AuthMechanism: G_config.MongodbAuthMechanism,
		AuthSource:    G_config.MongodbAuthSource,
		Username:      G_config.MongodbUser,
		Password:      G_config.MongodbPass,
	})
	clientOptions.SetConnectTimeout(time.Duration(G_config.MongodbTimeout) * time.Millisecond)
	if client, err = mongo.Connect(context.TODO(), G_config.MongodbURI, clientOptions); err != nil {
		fmt.Println("连接mongodb错误:", err.Error())
		return
	}
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}
	// 启动一个 mongodb 日志存储协程
	go G_logSink.writeLoop()
	return
}

func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		//队列满了就丢弃日志
	}
}
