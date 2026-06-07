package mtiming

//
// a MapReduce pseudo-application to test that workers
// execute map tasks in parallel.
//
// (Windows-native port: imported as a package by mrapps/apps. The original
// used syscall.Kill(pid, 0) to check whether a peer worker process was still
// alive; syscall.Kill does not exist on Windows. Since these timing tests
// never crash workers and run in a fresh temp dir each time, every matching
// mr-worker-* file corresponds to a live concurrent worker, so we simply
// count the files.)
//

import "6.5840/mr"
import "strings"
import "fmt"
import "os"
import "time"
import "sort"
import "io/ioutil"

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// how many other workers are running concurrently?
	// find them by scanning the directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			ret += 1
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {
	t0 := time.Now()
	ts := float64(t0.Unix()) + (float64(t0.Nanosecond()) / 1000000000.0)
	pid := os.Getpid()

	n := nparallel("map")

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("times-%v", pid),
		fmt.Sprintf("%.1f", ts)})
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("parallel-%v", pid),
		fmt.Sprintf("%d", n)})
	return kva
}

func Reduce(key string, values []string) string {
	//n := nparallel("reduce")

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)
	
	val := strings.Join(vv, " ")
	return val
}
