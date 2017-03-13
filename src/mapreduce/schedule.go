package mapreduce

import (
	"fmt"
	"sync"
)
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
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	var wg sync.WaitGroup
	for idx := 0; idx < ntasks; idx++ {
		file_str := ""
		if phase == mapPhase {
			file_str = mapFiles[idx]
		}
		wg.Add(1)
		s_fun := func(jobName string, file_str string, phase jobPhase, idx int, n_other int) {
			defer wg.Done()
			for {
				worker_name := <-registerChan
				tmp_arg := DoTaskArgs{jobName, file_str, phase, idx, n_other}
				res := call(worker_name, "Worker.DoTask", tmp_arg, nil)
				//fmt.Println(worker_name, " ", res, " ", file_str)
				// need to start a new goroutine to receive the worker addr
				go func(worker_name string) {
					registerChan<-worker_name
				} (worker_name)
				if res == true {
					break
				}
			}
		}
		go s_fun(jobName, file_str, phase, idx, n_other)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
