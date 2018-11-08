package worker

import (
	"context"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/options"
	"github.com/yenkeia/cron/common"
	"time"
)

// mongodb 存储日志
type LogSink struct {
	client        *mongo.Client
	logCollection *mongo.Collection
	logChan       chan *common.JobLog
}

var G_logSink *LogSink

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log *common.JobLog
	)

	for {
		select {
		case log = <-logSink.logChan:
			// TODO 把这条 log 写到 mongodb 中
			// logSink.logCollection.InsertOne
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
		return
	}
	//database = client.Database("cron")
	G_logSink = &LogSink{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
		logChan:       make(chan *common.JobLog, 1000),
	}
	// 启动一个 mongodb 日志存储协程
	go G_logSink.writeLoop()
}
