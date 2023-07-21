package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetOutput(io.Discard)
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := fmt.Sprintf("worker-%d-%s", time.Now().Nanosecond(), RandStringRunes(5))
	log.Printf("worker %s starts...", id)
	for {
		// apply tasks
		log.Printf("worker %s tries to apply task", id)
		task, reduceCount, err := applyTask(id)
		if err != nil {
			log.Printf("invoke coordinator failed, err:%v", err)
			return
		}
		if task == nil {
			log.Printf("no available tasks for worker:%s now, wait and retry", id)
			time.Sleep(time.Second)
			continue
		}
		if task.Type == 0 {
			log.Printf("worker %s exits...", id)
			return
		}

		// map task
		if task.Type == 1 {
			log.Printf("worker %s is assigned task:%v", id, task)
			executeMapTask(id, task, reduceCount, mapf)
			log.Printf("worker %s tries to submit task:%s", id, task.Id)
			if err := submitTask(id, task); err != nil {
				log.Printf("invoke coordinator failed, err:%v", err)
				return
			}
			log.Printf("worker %s submits task:%s successfully", id, task.Id)
			continue
		}

		// reduce task
		if task.Type == 2 {
			log.Printf("worker %s is assigned task:%v", id, task)
			executeReduceTask(id, task, reducef)
			log.Printf("worker %s tries to submit task:%s", id, task.Id)
			if err := submitTask(id, task); err != nil {
				log.Printf("invoke coordinator failed, err:%v", err)
				return
			}
			log.Printf("worker %s submits task:%s successfully", id, task.Id)
			continue
		}
		log.Printf("unknown task type:%d, id:%s", task.Type, task.Id)
		time.Sleep(time.Second)
	}
}

func executeMapTask(id string, task *Task, reduceCount int, mapf func(string, string) []KeyValue) {
	log.Printf("worker %s starts to execute map task:%s", id, task.Id)
	var outputs []string
	var files []*os.File
	for i := 0; i < reduceCount; i++ {
		output := fmt.Sprintf("%s-%s-%d", id, task.Id, i)
		f, _ := os.Create(output)
		files = append(files, f)
		outputs = append(outputs, output)
	}
	var intermediates []KeyValue
	for _, filename := range task.Inputs {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediates = append(intermediates, kva...)
		for _, kv := range intermediates {
			fmt.Fprintf(files[ihash(kv.Key)%reduceCount], "%v %v\n", kv.Key, kv.Value)
		}
	}
	for _, f := range files {
		f.Close()
	}
	task.OutPuts = outputs
	log.Printf("worker %s finishes to execute map task:%s", id, task.Id)
}

func executeReduceTask(id string, task *Task, reducef func(string, []string) string) {
	log.Printf("worker %s starts to execute reduce task:%s", id, task.Id)
	var intermediates []KeyValue
	for _, filename := range task.Inputs {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		lines, err := readLines(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		for _, line := range lines {
			parts := strings.Split(line, " ")
			kv := KeyValue{Key: parts[0], Value: parts[1]}
			intermediates = append(intermediates, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediates))

	os.Remove(task.Id)
	reduceFile, _ := os.Create(task.Id)
	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(reduceFile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}
	reduceFile.Close()
	log.Printf("worker %s finishes to execute reduce task:%s", id, task.Id)
}

func readLines(file *os.File) ([]string, error) {
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func applyTask(id string) (*Task, int, error) {
	req := ApplyTaskRequest{WorkerId: id}
	reply := ApplyTaskResponse{}
	method := "Coordinator.ApplyTask"
	ok := call(method, &req, &reply)
	if ok {
		return reply.Task, reply.ReduceCount, nil
	} else {
		return nil, 0, errors.New(fmt.Sprintf("invoke %s failed", method))
	}
}

func submitTask(id string, task *Task) error {
	req := SubmitTaskRequest{WorkerId: id, Task: task}
	reply := SubmitTaskResponse{}
	method := "Coordinator.SubmitTask"
	ok := call(method, &req, &reply)
	if ok {
		return nil
	} else {
		return errors.New(fmt.Sprintf("invoke %s failed", method))
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
