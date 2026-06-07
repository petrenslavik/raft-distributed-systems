package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type FunctionType int

const (
	MapTask FunctionType = iota
	ReduceTask
	SleepTask
)

type GetJobRequest struct{}

type GetJobResponse struct {
	TaskType   FunctionType
	TaskNumber int
	Buckets    int
	Maps       int
	Input      string
}

type FinishJobRequest struct {
	TaskNumber int
	TaskType   FunctionType
}

type FinishJobResponse struct{}
