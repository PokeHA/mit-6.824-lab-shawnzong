package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//从Coordinator获取任务并打印
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	workid := time.Now().UnixNano()

	for true {
		mrtask := GetTask()
		if mrtask.TaskName != "" {
			fmt.Println("worker ", workid, "成功获取到任务", mrtask.TaskName)
			doMapTask(mrtask, mapf)

			TaskDone(mrtask)

		} else {
			fmt.Println("任务获取失败，当前worker休眠1s")
			time.Sleep(time.Second * 10)
		}
	}
}

func GetTask() MRTask {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Println(reply.TaskName)
		return MRTask{reply.IsMap, reply.Seq, reply.TaskName, reply.NReduce}
	} else {
		return MRTask{}
	}
}

func TaskDone(mrtask MRTask) {
	args := TaskFinishedArgs{}
	reply := TaskFinishedReply{}
	args.IsMap = mrtask.IsMapTask
	args.TaskName = mrtask.TaskName
	args.Seq = mrtask.Seq

	ok := call("Coordinator.Finished", &args, &reply)
	if ok {
		fmt.Println("任务完成信息已传达")
	} else {
		fmt.Println("任务完成信息传递失败")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := GetTaskArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Seq)
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

func doMapTask(t MRTask, mapf func(string, string) []KeyValue) {
	nreduce := t.NReduce
	//intermediate := [][]KeyValue{}
	intermediate := make([][]KeyValue, nreduce)

	file, err := os.Open(t.TaskName)
	fmt.Println(t.TaskName, "已读取")

	if err != nil {
		log.Fatalf("cannot open %v", t.TaskName)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", t.TaskName)
	}
	file.Close()

	kva := mapf(t.TaskName, string(content))
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%nreduce] = append(intermediate[ihash(kv.Key)%nreduce], kv)
	}

	//对每个bucket进行排序
	for i := 0; i < len(intermediate); i++ {
		//TODO 验证对MapBucket的排序
		sort.Sort(ByKey(intermediate[i]))
	}

	//将多个bucket保存到不同的文件里去
	for i := 0; i < len(intermediate); i++ {
		oname := "mr-" + strconv.Itoa(t.Seq) + "-" + strconv.Itoa(i)
		_, ferr := os.Lstat(oname)
		if !os.IsNotExist(ferr) {
			fmt.Println("文件", oname, "已存在")
			continue
		}
		onametmp := oname + "-tmp"
		ofile, _ := os.Create(onametmp)
		bytes, _ := json.Marshal(intermediate[i])
		fmt.Fprintf(ofile, string(bytes))
		ofile.Close()
		//更改文件名
		err := os.Rename(onametmp, oname)
		if err == nil {
			fmt.Println("文件", oname, "创建成功")
		}
	}
}
