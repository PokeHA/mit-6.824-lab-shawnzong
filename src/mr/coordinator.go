package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	State                    int
	Lock                     sync.Mutex
	MapTaskInfos             []MRTask
	UnassignedMapTaskChannel chan MRTask
	AssignedMapTaskMap       map[int]MRTask
}

type MRTask struct {
	IsMapTask bool
	Seq       int
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
func (c *Coordinator) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.State == 0 {
		return errors.New("Coordinator is not Ready")
	}

	if c.State == 1 {

		if len(c.UnassignedMapTaskChannel) > 0 {
			//分发map任务
			mrtask := <-c.UnassignedMapTaskChannel
			c.AssignedMapTaskMap[mrtask.Seq] = mrtask

			reply.TaskName = mrtask.TaskName
			reply.Seq = mrtask.Seq
			reply.IsMap = mrtask.IsMapTask

			//设定定时任务，10s后检查任务是否完成
			go func() {
				tm := time.NewTimer(time.Second * 5)
				defer tm.Stop()
				<-tm.C
				c.Lock.Lock()
				defer c.Lock.Unlock()

				fmt.Println("定时任务启动")
				if _, exist := c.AssignedMapTaskMap[mrtask.Seq]; exist {
					//如果任务还在已分配map里
					c.UnassignedMapTaskChannel <- mrtask
					delete(c.AssignedMapTaskMap, mrtask.Seq)
				}

			}()

		} else {
			//c.Done()
		}

	} else {

	}

	return nil
}

// Worker通知Coordinator任务完成
func (c *Coordinator) Finished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if args.IsMap && c.State == 1 {
		delete(c.AssignedMapTaskMap, args.Seq)
		fmt.Println("Coordinator：Map任务已完成", args.TaskName)
		if len(c.AssignedMapTaskMap) == 0 && len(c.UnassignedMapTaskChannel) == 0 {
			fmt.Println("所有Map任务都处理完了")
		}
	}
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
	c.AssignedMapTaskMap = make(map[int]MRTask)
	// Your code here.
	//初始化任务列表
	c.UnassignedMapTaskChannel = make(chan MRTask, len(os.Args)-1)
	for seq, filename := range os.Args[1:] {
		fmt.Println(filename, "Map任务已加入")
		mrtask := MRTask{true, seq, filename}
		c.MapTaskInfos = append(c.MapTaskInfos, mrtask)
		c.UnassignedMapTaskChannel <- mrtask
	}

	c.State = 1

	c.server()
	return &c
}
