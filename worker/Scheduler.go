package worker

import "github.com/yenkeia/cron/common"

// 任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent              // etcd 任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan // 任务调度计划表
}

var G_scheduler *Scheduler

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted      bool
		err             error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: // 删除任务事件
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	}
}

// 调度协程
func (scheduler Scheduler) scheduleLoop() {
	var (
		jobEvent *common.JobEvent
	)
	// 定时任务 common.Job
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: // 监听任务变化事件
			// 对*内存中*维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		}
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler() {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
	}
	// 启动调度协程
	go G_scheduler.scheduleLoop()
	return
}
