package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const (
	TaskDuration = 10 * time.Second
)

type TaskState int

const (
	IdleState TaskState = iota
	InProgressState
	DoneState
)

type Task struct {
	state      TaskState
	taskNumber int
	file       string
	timer      *time.Timer
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapQueue    chan int
	reduceQueue chan int

	mapTasks    []*Task
	reduceTasks []*Task

	totalMaps    int
	totalReduces int

	finishedMaps    int
	finishedReduces int
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
//
// (Windows-native port: the transport is TCP on a loopback host:port instead
// of a Unix-domain socket, since Unix sockets and /tmp paths are awkward on
// Windows. "sockname" is therefore an address like "127.0.0.1:34567".)
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishedReduces == c.totalReduces
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	mapTasks := make([]*Task, len(files))
	mapQueue := make(chan int, len(files))

	reduceTasks := make([]*Task, nReduce)
	reduceQueue := make(chan int, nReduce)

	for index, file := range files {
		mapTasks[index] = &Task{
			file:       file,
			taskNumber: index,
		}
		mapQueue <- index
	}

	for index := range nReduce {
		reduceTasks[index] = &Task{
			taskNumber: index,
		}
		reduceQueue <- index
	}

	c := Coordinator{
		mapTasks:     mapTasks,
		mapQueue:     mapQueue,
		reduceTasks:  reduceTasks,
		reduceQueue:  reduceQueue,
		totalMaps:    len(files),
		totalReduces: nReduce,
	}

	c.server(sockname)
	return &c
}

func (c *Coordinator) GetJob(args *GetJobRequest, reply *GetJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finishedMaps != c.totalMaps {
		select {
		case taskIndex := <-c.mapQueue:
			task := c.mapTasks[taskIndex]
			reply.TaskType = MapTask
			reply.Buckets = c.totalReduces
			reply.Input = task.file
			reply.TaskNumber = task.taskNumber
			task.state = InProgressState
			c.scheduleRequeueLocked(taskIndex, task, c.mapQueue)
		default:
			reply.TaskType = SleepTask
		}
		return nil
	}

	select {
	case taskIndex := <-c.reduceQueue:
		task := c.reduceTasks[taskIndex]
		reply.TaskType = ReduceTask
		reply.Buckets = c.totalReduces
		reply.TaskNumber = task.taskNumber
		reply.Maps = c.totalMaps
		task.state = InProgressState
		c.scheduleRequeueLocked(taskIndex, task, c.reduceQueue)
	default:
		reply.TaskType = SleepTask
	}
	return nil
}

func (c *Coordinator) OnJobFinish(args *FinishJobRequest, reply *FinishJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// assuming taskNumber equals index
	switch args.TaskType {
	case MapTask:
		if c.mapTasks[args.TaskNumber].state != DoneState {
			c.finishedMaps++
		}
		c.mapTasks[args.TaskNumber].state = DoneState
	case ReduceTask:
		if c.reduceTasks[args.TaskNumber].state != DoneState {
			c.finishedReduces++
		}
		c.reduceTasks[args.TaskNumber].state = DoneState
	}

	return nil
}

func (c *Coordinator) scheduleRequeueLocked(taskIndex int, task *Task, queue chan int) {
	timer := task.timer
	if timer != nil {
		timer.Stop()
	}
	timer = time.NewTimer(TaskDuration)
	task.timer = timer
	go func(c *Coordinator) {
		<-timer.C
		c.mu.Lock()
		defer c.mu.Unlock()
		if task.state != DoneState {
			queue <- taskIndex
			task.state = IdleState
		}
	}(c)
}
