package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 任务
type TaskStatus int
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
)
const (
	Idle       TaskStatus = iota // 未分配
	InProgress                   // 已分配，正在处理
	Fail
	Completed // 已完成
)

type Task struct {
	ID          int    // 任务ID
	Files       string // 输入文件(仅Map任务需要)
	ReduceFiles []string
	Status      TaskStatus
	FreshTime   time.Time // 超时时间(用于处理worker崩溃)
	Type        TaskType
}

// 协调器
type WorkPhase int

const (
	Begin WorkPhase = iota
	Mapping
	Reducing
	Done
)

type Coordinator struct {
	//互斥锁、工作阶段、任务栏
	Phase          WorkPhase
	MapTaskList    []*Task
	ReduceTaskList []*Task
	nReduce        int        // reduce任务数量
	mu             sync.Mutex // 一个互斥锁就够了
	mapDone        bool
	reduceDone     bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Phase == Mapping {
		for i, t := range c.MapTaskList {
			if t.Status != Idle {
				continue
			}
			//分派任务 -保护临界资源
			c.MapTaskList[i].Status = InProgress
			c.MapTaskList[i].FreshTime = time.Now()
			reply.AssginedTask = c.MapTaskList[i]
			reply.NReduce = c.nReduce
			reply.Finished = false
			return nil
		}
	} else if c.Phase == Reducing {
		for i, t := range c.ReduceTaskList {
			if t.Status != Idle {
				continue
			}
			c.ReduceTaskList[i].Status = InProgress
			c.ReduceTaskList[i].FreshTime = time.Now()
			reply.AssginedTask = c.ReduceTaskList[i]
			reply.NReduce = c.nReduce
			reply.Finished = false
			return nil
		}
		done := true
		for _, t := range c.ReduceTaskList {
			if t.Status != Completed {
				done = false
			}
		}
		if done {
			reply.Finished = true
		} else {
			reply.Finished = false
		}
		reply.AssginedTask = nil
		return nil
	} else if c.Phase == Done {
		reply.Finished = true
		return nil
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Phase == Mapping && args.TaskType == Map {
		for i, _ := range c.MapTaskList {
			if c.MapTaskList[i].ID == args.TaskId && c.MapTaskList[i].Status == InProgress {
				c.MapTaskList[i].Status = Completed
			}
		}
		// 任务都完成时 协调器更新状态
		finish := true
		for _, t := range c.MapTaskList {
			if t.Status != Completed {
				finish = false
				break
			}
		}
		if finish {
			for i := 0; i < c.nReduce; i++ {
				reduceFile := make([]string, len(c.MapTaskList))
				for j := 0; j < len(reduceFile); j++ {
					reduceFile[j] = "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
				}
				c.ReduceTaskList[i].ReduceFiles = reduceFile
			}
			c.Phase = Reducing
			c.mapDone = true
		}
	} else if c.Phase == Reducing && args.TaskType == Reduce {
		for i, _ := range c.ReduceTaskList {
			if c.ReduceTaskList[i].ID == args.TaskId && c.ReduceTaskList[i].Status == InProgress {
				c.ReduceTaskList[i].Status = Completed
			}
		}
		finish := true
		for _, t := range c.ReduceTaskList {
			if t.Status != Completed {
				finish = false
				break
			}
		}
		if finish {
			c.Phase = Done
			c.reduceDone = true
		}
	} else {
		return fmt.Errorf("report task fail: error Task Type")
	}
	return nil
}

func (c *Coordinator) HealthUpdata(args *HealthArgs, reply *HealthReply) error {
	if args.TaskType == Map {
		c.MapTaskList[args.TaskId].FreshTime = time.Now()
	} else if args.TaskType == Reduce {
		c.ReduceTaskList[args.TaskId].FreshTime = time.Now()
	}
	return nil
}

func (c *Coordinator) HealthCheck() {
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		now := time.Now()

		// 检查Map任务
		if c.Phase == Mapping {
			for _, task := range c.MapTaskList {
				if task.Status == InProgress && now.After(task.FreshTime.Add(1*time.Second)) {
					task.Status = Idle
				}
			}
		}

		// 检查Reduce任务
		if c.Phase == Reducing {
			for _, task := range c.ReduceTaskList {
				if task.Status == InProgress && now.After(task.FreshTime.Add(1*time.Second)) {
					task.Status = Idle
				}
			}
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.Phase == Done
	if ret {
		time.Sleep(1 * time.Second)
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase:      Begin,
		nReduce:    nReduce,
		mapDone:    false,
		reduceDone: false,
	}
	//首先应该根据文件名划分map任务
	mTaskList := divideMapTask(files)
	c.MapTaskList = mTaskList
	//根据nReduce初始化reduce任务
	rTaskList := make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		rTaskList[i] = &Task{
			ID:     i,
			Status: Idle,
			Type:   Reduce,
		}
	}
	c.ReduceTaskList = rTaskList
	c.Phase = Mapping
	go c.HealthCheck()
	c.server()
	return &c
}

func divideMapTask(files []string) []*Task {
	TaskList := make([]*Task, len(files))
	for i, file := range files {
		TaskList[i] = &Task{
			Files:  file,
			Status: Idle,
			ID:     i,
			Type:   Map,
		}
	}
	return TaskList
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
