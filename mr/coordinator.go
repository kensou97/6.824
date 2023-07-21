package mr

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

func init() {
	log.SetOutput(io.Discard)
}

type Coordinator struct {
	mu               sync.Mutex
	idleMapTasks     map[string]Task
	runningMapTasks  map[string]Task
	finishedMapTasks map[string]Task

	idleReduceTasks     map[string]Task
	runningReduceTasks  map[string]Task
	finishedReduceTasks map[string]Task

	reduceCount int
}

func (c *Coordinator) ApplyTask(req *ApplyTaskRequest, reply *ApplyTaskResponse) error {
	log.Printf("worker %s applies task", req.WorkerId)
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceCount = c.reduceCount
	if len(c.finishedReduceTasks) == c.reduceCount {
		log.Println("all tasks are finished...")
		reply.Task = &Task{Type: 0}
		return nil
	}

	// assign idle map task to worker
	if len(c.idleMapTasks) > 0 {
		for id, task := range c.idleMapTasks {
			log.Printf("assign task %s to worker %s", id, req.WorkerId)
			delete(c.idleMapTasks, id)
			task.startTime = time.Now()
			c.runningMapTasks[id] = task
			reply.Task = &task
			return nil
		}
	}

	// no idle map task
	if len(c.runningMapTasks) != 0 {
		log.Printf("waiting map tasks finish, no available task for worker %s now", req.WorkerId)
		return nil
	}

	// all map tasks are completed
	if len(c.finishedMapTasks) != 0 {
		log.Println("init reduce tasks...")

		idleReduceTasks := map[string]Task{}
		for i := 0; i < c.reduceCount; i++ {
			var inputs []string
			for _, task := range c.finishedMapTasks {
				inputs = append(inputs, task.OutPuts[i])
			}
			reduce := Task{Id: fmt.Sprintf("mr-out-%d", i), Type: 2, Inputs: inputs}
			idleReduceTasks[reduce.Id] = reduce
		}
		c.finishedMapTasks = nil
		c.idleReduceTasks = idleReduceTasks
		log.Printf("%d reduce task inited...", len(c.idleReduceTasks))
	}

	// assign idle reduce task to worker
	if len(c.idleReduceTasks) > 0 {
		for id, task := range c.idleReduceTasks {
			log.Printf("assign task %s to worker %s", id, req.WorkerId)
			delete(c.idleReduceTasks, id)
			task.startTime = time.Now()
			c.runningReduceTasks[id] = task
			reply.Task = &task
			return nil
		}
	}
	log.Printf("no available task for worker %s now", req.WorkerId)
	return nil
}

func (c *Coordinator) SubmitTask(req *SubmitTaskRequest, _ *SubmitTaskResponse) error {
	log.Printf("worker %s submits task %s", req.WorkerId, req.Task.Id)
	c.mu.Lock()
	defer c.mu.Unlock()

	if req.Task.Type == 1 {
		task, ok := c.runningMapTasks[req.Task.Id]
		if !ok {
			log.Printf("task %s is not running, maybe it's already finished by another worker", req.Task.Id)
			return nil
		}
		delete(c.runningMapTasks, task.Id)
		task.OutPuts = req.Task.OutPuts
		c.finishedMapTasks[task.Id] = task
		return nil
	}

	if req.Task.Type == 2 {
		task, ok := c.runningReduceTasks[req.Task.Id]
		if !ok {
			log.Printf("task %s is not running, maybe it's already finished by another worker", req.Task.Id)
			return nil
		}
		delete(c.runningReduceTasks, task.Id)
		task.OutPuts = req.Task.OutPuts
		c.finishedReduceTasks[task.Id] = task
		return nil
	}
	log.Printf("unknown task type:%d,id:%s", req.Task.Type, req.Task.Id)
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.finishedReduceTasks) == c.reduceCount
}

var timeout = 10

func (c *Coordinator) detectTimeoutTasks() {
	for {
		c.mu.Lock()
		for id, task := range c.runningMapTasks {
			if time.Since(task.startTime) > time.Duration(timeout)*time.Second {
				log.Printf("task %s is timeout, move to idle", id)
				delete(c.runningMapTasks, id)
				c.idleMapTasks[id] = task
			}
		}
		for id, task := range c.runningReduceTasks {
			if time.Since(task.startTime) > time.Duration(timeout)*time.Second {
				log.Printf("task %s is timeout, move to idle", id)
				delete(c.runningReduceTasks, id)
				c.idleReduceTasks[id] = task
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		reduceCount:         nReduce,
		idleMapTasks:        map[string]Task{},
		runningMapTasks:     map[string]Task{},
		finishedMapTasks:    map[string]Task{},
		idleReduceTasks:     map[string]Task{},
		runningReduceTasks:  map[string]Task{},
		finishedReduceTasks: map[string]Task{},
	}

	idleMapTasks := map[string]Task{}
	log.Println("init map tasks...")
	for i, file := range files {
		task := Task{
			Id:     fmt.Sprintf("map-%d", i),
			Type:   1,
			Inputs: []string{file},
		}
		idleMapTasks[task.Id] = task
	}
	c.idleMapTasks = idleMapTasks
	log.Printf("%d map tasks inited...", len(idleMapTasks))
	go c.detectTimeoutTasks()
	c.server()
	return &c
}
