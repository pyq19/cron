package worker

import (
	"context"
	"github.com/yenkeia/cron/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// 管理任务

// 任务管理器
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var G_jobMgr *JobMgr

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)

	// 1. get /cron/jobs/ 目录下所有任务 并获知当前集群的 revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	// 当前有哪些任务
	for _, kvpair = range getResp.Kvs {
		// 反序列化 json 得到 job
		if job, err = common.UnpackJob(kvpair.Value); err == nil { // 反解成功
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// 同步给 scheduler (调度协程)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 2. 从 revision 向后监听变化事件
	go func() { // 监听协程
		watchStartRevision = getResp.Header.Revision + 1 // 从 GET 时刻的后续版本开始监听变化
		// 监听 /cron/jobs/ 目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					// 构建*更新*事件 event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE: // 任务被删除
					// DELETE /cron/jobs/job10
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					// 构建*删除*事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				// 变化推给 scheduler
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}

// 初始化管理，建立和 etcd 的连接
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时时间
	}
	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}
	// 得到 kv 和 lease 的 API 子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	// 赋值单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}
	// 启动任务监听
	G_jobMgr.watchJobs()
	return
}
