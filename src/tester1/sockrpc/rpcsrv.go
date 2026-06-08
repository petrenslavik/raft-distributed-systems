package sockrpc

import (
	"log"
	"net"
	"os"
	"path/filepath"

	"6.5840/labrpc"
	"6.5840/tester1/demux"
)

func SockName(endName string) string {
	// Windows-native port: the stock path "/tmp/6.5840-..." doesn't exist on
	// Windows. Use the OS temp dir instead; AF_UNIX works on Windows 10+ with
	// a valid filesystem path. Both the tester and the daemon processes
	// resolve os.TempDir() to the same location, so the name stays consistent
	// across them.
	return filepath.Join(os.TempDir(), "6.5840-"+endName)
}

type RPCSrv struct {
	sock string
	l    net.Listener
	srv  *labrpc.Server
}

func NewRPCSrv(sock string) *RPCSrv {
	rpcs := &RPCSrv{sock: sock}
	rpcs.srv = labrpc.MakeServer()
	go rpcs.listen()
	return rpcs
}

func (rpcs *RPCSrv) Close() {
	rpcs.l.Close()
}

func (rpcs *RPCSrv) Name() string {
	return rpcs.sock
}

func (rpcs *RPCSrv) AddService(svc any) {
	rpcs.srv.AddService(labrpc.MakeService(svc))
}

func (rpcs *RPCSrv) listen() {
	os.Remove(SockName(rpcs.sock)) // clear any stale socket file (Windows may not unlink on close)
	l, err := net.Listen("unix", SockName(rpcs.sock))
	if err != nil {
		log.Fatal("tester listen error:", err)
	}
	//log.Printf("rpcs listen %q", sockName(rpcs.sock))
	rpcs.l = l
	for {
		c, err := l.Accept()
		if err != nil {
			//log.Printf("rpcs accept err %v", err)
			return
		}
		t := demux.NewTransport(c)
		demux.NewDemuxSrv(rpcs.sock, rpcs, t)
	}
}

func (rpcs *RPCSrv) ServeRequest(clntEnd string, b []byte) ([]byte, bool) {
	req := RPCArgs{}
	labrpc.Unmarshall(b, &req)
	//log.Printf("%q: dispatch %v", rpcs.sock, req.Method)
	rep, ok := rpcs.srv.Dispatch(rpcs.sock, req.Method, clntEnd, req.Args)
	return rep, ok
}
