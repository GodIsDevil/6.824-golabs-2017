package mapreduce

import (
	"fmt"
)

const RpcName = "Worker.DoTask"

const MaxWorkerFailures = 5

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	taskChannel := make(chan int, ntasks)
	go func(taskSize int) {
		for i := 0; i < taskSize; i++ {
			taskChannel <- i
		}
	}(ntasks)

	done := make(chan string, ntasks)

	remainTaskNumber := ntasks
	for remainTaskNumber > 0 {
		select {
		case workerRpcAddress := <-registerChan:
			{
				go func(workerRpcAddress string) {
					failureCounts := 0
					for taskNumber := range taskChannel {
						doTaskArgs := DoTaskArgs{JobName: jobName, File: mapFiles[taskNumber], Phase: phase,
							TaskNumber: taskNumber, NumOtherPhase: n_other}
						ok := call(workerRpcAddress, RpcName, doTaskArgs, nil)
						if ok {
							done <- "done"
						} else {
							taskChannel <- taskNumber
							failureCounts++
							if failureCounts >= MaxWorkerFailures {
								fmt.Printf("Schedule: %v, Worker %s failed %d times and will no longer be used\n", phase, workerRpcAddress, MaxWorkerFailures)
								break
							}
						}
					}
				}(workerRpcAddress)
			}
		case <-done:
			remainTaskNumber--
		}
	}
	close(taskChannel)
	close(done)
	fmt.Printf("Schedule: %v phase done\n", phase)

}
