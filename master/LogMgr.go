package master

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/yenkeia/cron/common"
	"time"
)

// mongodb 日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var G_logMgr *LogMgr // 日志管理 单例

func InitLogMgr() (err error) {
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

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  mongo.Cursor
		jobLog  *common.JobLog
		skip64  int64
		limit64 int64
	)
	logArr = make([]*common.JobLog, 0)
	skip64 = int64(skip)
	limit64 = int64(limit)

	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	// 查询
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{Skip: &skip64, Limit: &limit64, Sort: logSort}); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		// 反序列化 bson 到 jobLog 对象
		if err = cursor.Decode(jobLog); err != nil {
			continue // 出错说明日志不合法, continue 忽略
		}
		logArr = append(logArr, jobLog)
	}

	return
}
