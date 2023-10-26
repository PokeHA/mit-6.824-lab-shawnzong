package mr

import (
	"errors"
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	State int
}

type MRTask struct {
	IsMapTask bool
	Seq       uint
	TaskName  string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *GetTaskArgs, reply *GetTaskReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// Worker从Coordinator获取Task
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.State == 0 {
		return errors.New("Coordinator is not Ready")
	}

	return nil
}

// Worker通知Coordinator任务完成
func (c *Coordinator) Finished(args *GetTaskArgs, reply *GetTaskReply) error {

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//初始化任务列表
	for seq, filename := range os.Args[1:] {
		fmt.Println(filename, "Map任务已加入")

	}

	c.server()
	return &c
}
