package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// 1. 获取任务
		task, nReduce, finished := GetTask()
		ctx, cancel := context.WithCancel(context.Background())
		if finished {
			return
		}
		if task == nil {
			time.Sleep(300 * time.Millisecond)
			cancel()
			continue
		}
		if task.Type == Map {

			go func() {
				defer cancel()
				HealthUpdate(ctx, task)
			}()

			MapTask(mapf, task, nReduce)
		} else if task.Type == Reduce {
			go func() {
				defer cancel()
				HealthUpdate(ctx, task)
			}()

			ReduceTask(reducef, task)
		} else {
			break
		}

		cancel()
	}
}

func MapTask(mapf func(string, string) []KeyValue, task *Task, nReduce int) {
	intermediate := []KeyValue{}
	filename := task.Files
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	HashKV := make(map[int][]KeyValue)

	for _, v := range intermediate {
		index := ihash(v.Key) % nReduce
		HashKV[index] = append(HashKV[index], v)
	}

	for i := 0; i < nReduce; i++ {
		data, err := json.MarshalIndent(HashKV[i], "", "     ")
		if err != nil {
			return
		}

		err = os.WriteFile("mr-"+strconv.Itoa(task.ID)+"-"+strconv.Itoa(i), data, 0644)
		if err != nil {
			panic(err)
		}
	}
	ReportTask(task)
}
func ReduceTask(reducef func(string, []string) string, task *Task) {

	intermediate := []KeyValue{}

	for _, filename := range task.ReduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		var data []KeyValue
		json.Unmarshal(content, &data)
		intermediate = append(intermediate, data...)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(task.ID)
	ofile, err := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	ReportTask(task)
}
func HealthUpdate(ctx context.Context, task *Task) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			args := HealthArgs{}
			args.TaskId = task.ID
			args.TaskType = task.Type
			reply := HealthReply{}

			call("Coordinator.HealthUpdate", &args, &reply)
			time.Sleep(800 * time.Millisecond)
		}
	}
}
func GetTask() (*Task, int, bool) {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Println("Coordinator.AssignTask failed")
		return nil, -1, false
	}
	return reply.AssginedTask, reply.NReduce, reply.Finished
}

func ReportTask(task *Task) {
	args := ReportArgs{TaskType: task.Type, TaskId: task.ID}
	reply := ReportReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		fmt.Println("Coordinator.ReportTask failed")
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
