package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//
// (Windows-native port: the application's Map/Reduce functions are looked up
// from the static mrapps/apps registry instead of a .so plugin, since
// -buildmode=plugin is unavailable on Windows. The "xxx.so" argument is still
// accepted; only its base name is used as the lookup key.)
//

import "6.5840/mr"
import "6.5840/mrapps/apps"
import "os"
import "fmt"

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so sockname\n")
		os.Exit(1)
	}

	mapf, reducef := apps.Load(os.Args[1])

	mr.Worker(os.Args[2], mapf, reducef)
}
