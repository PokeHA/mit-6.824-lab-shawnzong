package mr

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	State                 int
	Lock                  sync.Mutex
	MapTaskInfos          []MRTask
	ReduceTaskInfos       []MRTask
	UnassignedTaskChannel chan MRTask
	AssignedTaskMap       map[int]MRTask
	NReduce               int
}

type MRTask struct {
	IsMapTask bool
	Seq       int
	TaskName  string
	NReduce   int
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

	if c.State == 1 || c.State == 2 {
		if len(c.UnassignedTaskChannel) > 0 {
			//分发map或reduce任务
			mrtask := <-c.UnassignedTaskChannel
			c.AssignedTaskMap[mrtask.Seq] = mrtask

			reply.TaskName = mrtask.TaskName
			reply.Seq = mrtask.Seq
			reply.IsMap = mrtask.IsMapTask
			reply.NReduce = c.NReduce

			//设定定时任务，10s后检查任务是否完成
			go func() {
				tm := time.NewTimer(time.Second * 5)
				defer tm.Stop()
				<-tm.C
				c.Lock.Lock()
				defer c.Lock.Unlock()

				//TODO 弄清楚超时判断的加锁时机
				if _, exist := c.AssignedTaskMap[mrtask.Seq]; exist {
					//如果任务还在已分配map里
					c.UnassignedTaskChannel <- mrtask
					delete(c.AssignedTaskMap, mrtask.Seq)
					fmt.Println("任务", mrtask.TaskName, "超时，重新放回队列中")
				}
			}()
		}
	}

	return nil
}

// Worker通知Coordinator任务完成
func (c *Coordinator) Finished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if args.IsMap && c.State == 1 {
		delete(c.AssignedTaskMap, args.Seq)
		fmt.Println("Coordinator：Map任务已完成", args.TaskName)
		if len(c.AssignedTaskMap) == 0 && len(c.UnassignedTaskChannel) == 0 {
			fmt.Println("所有Map任务都处理完了")
			c.State = 0
			//准备reduce任务
			prepareReduceTask(c)
		}
	}

	if !args.IsMap && c.State == 2 {
		delete(c.AssignedTaskMap, args.Seq)
		fmt.Println("Coordinator：Reduce任务已完成", args.TaskName)
		if len(c.AssignedTaskMap) == 0 && len(c.UnassignedTaskChannel) == 0 {
			fmt.Println("所有Reduce任务都处理完了")
			c.State = 0
			//TODO 准备退出程序

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

	//TODO 删除当前文件夹下所有mr开头的文件
	mfiles, err := os.ReadDir("./")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	for _, file := range mfiles {
		found, err := regexp.MatchString("^mr-", file.Name())
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Println(file.Name(), "已删除")
			os.Remove(file.Name())
		}
	}

	c := Coordinator{}
	c.NReduce = nReduce
	//准备Map任务
	prepareMapTask(&c, files)
	//prepareReduceTask(&c)
	c.server()
	return &c
}

// 准备Map任务
func prepareMapTask(c *Coordinator, files []string) {
	c.AssignedTaskMap = make(map[int]MRTask)
	// Your code here.
	//初始化任务列表
	channelsize := len(files)
	if c.NReduce > channelsize {
		channelsize = c.NReduce
	}
	c.UnassignedTaskChannel = make(chan MRTask, channelsize)
	//for seq, filename := range os.Args[1:] {
	for seq, filename := range files {
		fmt.Println(filename, "Map任务已加入")
		mrtask := MRTask{true, seq, filename, c.NReduce}
		c.MapTaskInfos = append(c.MapTaskInfos, mrtask)
		c.UnassignedTaskChannel <- mrtask
	}
	c.State = 1
}

// 准备Reduce任务
func prepareReduceTask(c *Coordinator) {
	fmt.Println("开始处理Reduce任务")
	//Reduce任务应该就是nReduce个
	for i := 0; i < c.NReduce; i++ {
		c.ReduceTaskInfos = append(c.ReduceTaskInfos, MRTask{false, i, "mr-*-" + strconv.Itoa(i), c.NReduce})
	}
	for _, t := range c.ReduceTaskInfos {
		fmt.Println("Map任务", t.TaskName, "已加入")
		c.UnassignedTaskChannel <- t
	}
	c.State = 2
}
