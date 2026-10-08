package mr

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	TasksMetaData  map[int]*TaskMetaData
	TaskList       []Task
	TotalTasks     atomic.Int32
	CompletedTasks atomic.Int32
	MapPhaseStatus byte
	NReduce        uint16
	Logger         *slog.Logger
	Completed      bool
	mu             sync.Mutex
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	return c.Completed
}

const (
	TaskTypeMap    byte = 1
	TaskTypeReduce byte = 2
	TaskTypeWait   byte = 3
)

const (
	TaskStatusUnProcessed byte = 1
	TaskStatusInProgress  byte = 2
	TaskStatusProcessed   byte = 3
)

const (
	PhaseStatusNotStarted byte = 0
	PhaseStatusStarted    byte = 1
	PhaseStatusCompleted  byte = 2
)

type Task struct {
	Id             int
	Type           byte
	FileIdentifier string
	NReduce        int
}

type TaskMetaData struct {
	task        *Task
	Status      byte
	monitorChan chan byte
}

var TaskId atomic.Int32

type ReportArgs struct {
	Task Task
}
type ReportReply struct {
	Ok bool
}

// func IsValidTask(task Task) bool {
// 	if task.Type != TaskTypeReduce && task.Type != TaskTypeMap {
// 		return false
// 	}
// 	validPath := fs.ValidPath(task.FileIdentifier)
// 	if !validPath {
// 		return false
// 	}
// 	return true
// }

func (c *Coordinator) AddTask(task Task) bool {

	TaskId.Add(1)
	task.Id = int(TaskId.Load())

	c.TasksMetaData[task.Id] = &TaskMetaData{
		task:        &task,
		monitorChan: make(chan byte),
		Status:      TaskStatusUnProcessed,
	}
	c.TaskList = append(c.TaskList, task)
	c.TotalTasks.Add(1)
	return true
}

func (c *Coordinator) StartReducePhase() {
	for i := 0; i < int(c.NReduce); i++ {
		task := Task{
			Type:           TaskTypeReduce,
			FileIdentifier: strconv.Itoa(i),
		}
		c.AddTask(task)
	}
}
func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	if args.Task.Type == TaskTypeMap && c.MapPhaseStatus == PhaseStatusCompleted {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	meta, ok := c.TasksMetaData[args.Task.Id]
	if !ok {
		return fmt.Errorf("internal coordinator error")
	}
	if meta.Status == TaskStatusProcessed || meta.Status == TaskStatusUnProcessed {
		return fmt.Errorf("task is either completed or not picked for processing")
	}

	meta.Status = TaskStatusProcessed
	meta.monitorChan <- TaskStatusUnProcessed
	c.CompletedTasks.Add(1)
	if c.CompletedTasks.Load() == c.TotalTasks.Load() {
		if c.MapPhaseStatus != PhaseStatusCompleted {
			c.MapPhaseStatus = PhaseStatusCompleted
			c.StartReducePhase()
		} else {
			c.Completed = true
		}
	}

	reply.Ok = true
	return nil
}

func (c *Coordinator) monitorTask(metaData *TaskMetaData) {
	select {
	case status := <-metaData.monitorChan:
		if status == TaskStatusProcessed {
			c.mu.Lock()
			metaData.Status = status
			c.mu.Unlock()
		}
		c.Logger.Debug("recieved unexpected task status from worker")
	case <-time.After(time.Second * 10):
		c.mu.Lock()
		metaData.Status = TaskStatusUnProcessed
		c.TaskList = append(c.TaskList, *metaData.task)
		c.mu.Unlock()
	}
}

func (c *Coordinator) RequestTask(workerId int, task *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.TaskList) == 0 {
		task.Id = 0
		task.FileIdentifier = ""
		task.Type = TaskTypeWait
		return nil
	}
	tempTask := c.TaskList[0]
	c.TaskList = c.TaskList[1:]
	*task = tempTask
	//and change the status of task to in progress in metadata
	metaData, ok := c.TasksMetaData[tempTask.Id]
	if !ok {
		return fmt.Errorf("internal cordinator error")
	}
	metaData.Status = TaskStatusInProgress
	c.TasksMetaData[task.Id] = metaData
	go c.monitorTask(metaData)
	return nil
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		TasksMetaData:  make(map[int]*TaskMetaData, 0),
		TaskList:       make([]Task, 0),
		TotalTasks:     atomic.Int32{},
		CompletedTasks: atomic.Int32{},
		MapPhaseStatus: PhaseStatusNotStarted,
		NReduce:        uint16(nReduce),
		Logger:         slog.New(slog.NewTextHandler(os.Stdout, nil)),
		Completed:      false,
		mu:             sync.Mutex{},
	}
	c.mu.Lock()

	for _, filename := range files {
		task := Task{
			Type:           TaskTypeMap,
			FileIdentifier: filename,
			NReduce:        nReduce,
		}
		c.AddTask(task)
	}
	c.MapPhaseStatus = PhaseStatusStarted
	c.mu.Unlock()
	c.server(sockname)
	return c
}
