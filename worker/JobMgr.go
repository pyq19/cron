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
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var G_jobMgr *JobMgr

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp *clientv3.GetResponse
		kvpair  *mvccpb.KeyValue
		job     *common.Job
	)

	// 1. get /cron/jobs/ 目录下所有任务 并获知当前集群的 revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	// 当前由哪些任务
	for _, kvpair = range getResp.Kvs {
		// 反序列化 json 得到 job
		if job, err = common.UnpackJob(kvpair.Value); err == nil { // 反解成功
			// TODO: 把 job 同步给 scheduler (调度协程)
		}
	}

	// 2. 从 revision 向后监听变化事件

	return
}

// 初始化管理，建立和 etcd 的连接
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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
	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}
