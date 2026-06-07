package mr

//
// Test helpers for the MapReduce lab.
//
// Windows-native port: the stock version shells out to the Unix tools
// `find`, `sort`, and `cmp`, uses Unix-domain sockets under /tmp, and runs
// extension-less binaries. This version uses pure-Go equivalents, a TCP
// loopback "host:port" socket name, and adds the ".exe" suffix on Windows.
// None of the student-facing logic (mr/coordinator.go, mr/worker.go) lives
// here; only the given test harness was ported.
//

import (
	"bytes"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

var tmp string

// exe returns the path of a built helper binary, adding the platform's
// executable suffix (".exe" on Windows).
func exe(p string) string {
	if runtime.GOOS == "windows" {
		return p + ".exe"
	}
	return p
}

func startWorker(app string, i int, c chan int, sock string) {
	worker := exec.Command(exe("../../main/mrworker"), append([]string{app}, sock)...)
	worker.Stderr = os.Stderr
	worker.Stdout = os.Stdout
	worker.Dir = tmp
	if err := worker.Start(); err != nil {
		log.Fatalf("mr failed %v", err)
	}
	go func(cmd *exec.Cmd, i int) {
		cmd.Wait()
		if c != nil {
			c <- i
		}
	}(worker, i)
}

// Run MapReduce: start a coordinator and several workers,
// and wait for the coordinator being done
func runMRchan(files []string, app string, n int, c chan int, sock string) {
	coord := exec.Command(exe("../main/mrcoordinator"), append([]string{sock}, files...)...)
	coord.Stderr = os.Stderr
	coord.Stdout = os.Stdout
	if err := coord.Start(); err != nil {
		log.Fatalf("mr failed %v", err)
	}

	// give the coordinator time to create the sockets.
	time.Sleep(1 * time.Second)

	for i := 0; i < n; i++ {
		startWorker(app, i, c, sock)
	}
	if err := coord.Wait(); err != nil {
		log.Fatalf("Wait %v", err)
	}
	if c != nil {
		c <- n
	}
}

func runMR(files []string, app string, n int) {
	sock := coordinatorSock()
	runMRchan(files, app, n, nil, sock)
}

func RandString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// Cook up a unique-ish TCP loopback address for the coordinator.
// (Windows-native port: was a UNIX-domain socket path under /tmp.)
func coordinatorSock() string {
	b := make([]byte, 2)
	crand.Read(b)
	port := 20000 + (int(b[0])<<8|int(b[1]))%20000 // 20000..39999
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// sortLinesToFile reads the lines of every input file, sorts them
// lexicographically, and writes them to out. Replaces the Unix `sort`.
// Using the same Go sort for both the "correct" and the "merged" output
// guarantees they are byte-identical when the reduce results agree.
func sortLinesToFile(inputs []string, out string) {
	var lines []string
	for _, in := range inputs {
		data, err := os.ReadFile(in)
		if err != nil {
			log.Fatalf("read %v failed err %v", in, err)
		}
		text := strings.TrimRight(string(data), "\n")
		if text == "" {
			continue
		}
		lines = append(lines, strings.Split(text, "\n")...)
	}
	sort.Strings(lines)
	f, err := os.Create(out)
	if err != nil {
		log.Fatalf("create %v failed err %v", out, err)
	}
	defer f.Close()
	for _, l := range lines {
		fmt.Fprintln(f, l)
	}
}

// Generate correct output for a test
func mkCorrectOutput(files []string, app, out string) {
	args := append([]string{app}, files...)
	cmd := exec.Command(exe("../../main/mrsequential"), args...)
	cmd.Dir = tmp
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("mrsequential %v failed err %v", args, err)
	}
	src := filepath.Join(tmp, "mr-out-0")
	sortLinesToFile([]string{src}, filepath.Join(tmp, out))
	if err := os.Remove(src); err != nil {
		log.Fatalf("Remove failed err %v", err)
	}
}

func mergeOutput(out string) {
	files := findFiles(tmp, "mr-out-[0-9]")
	if len(files) < 1 {
		log.Fatalf("reduce created no mr-out-X output files!")
	}
	sortLinesToFile(files, filepath.Join(tmp, out))
}

// findFiles returns the files directly under dir whose name matches the glob
// pattern s. Replaces the Unix `find dir -type f -name s` (none of the call
// sites need recursion). Returned paths are prefixed with dir, sorted.
func findFiles(dir, s string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("ReadDir %v failed err %v\n", dir, err)
		return nil
	}
	files := []string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ok, err := filepath.Match(s, e.Name())
		if err != nil {
			log.Fatalf("bad pattern %q: %v", s, err)
		}
		if ok {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files
}

func findFilesPre(dir, s, pre string) []string {
	files := findFiles(dir, s)
	for i, f := range files {
		files[i] = filepath.Join("..", f)
	}
	return files
}

func mkOut() {
	tmp = "mr-tmp-" + RandString(8)
	os.Mkdir(tmp, 0755)
}

func cleanup() {
	files := findFiles(tmp, "mr-*")
	for _, f := range files {
		os.Remove(f)
	}
	os.Remove(tmp)
}

func runCmp(t *testing.T, f1, f2, msg string) {
	a, err := os.ReadFile(filepath.Join(tmp, f1))
	if err != nil {
		t.Fatalf("%s (read %s: %v)", msg, f1, err)
	}
	b, err := os.ReadFile(filepath.Join(tmp, f2))
	if err != nil {
		t.Fatalf("%s (read %s: %v)", msg, f2, err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf(msg)
	}
}

func countPatternFile(f, p string) int {
	data, err := os.ReadFile(f)
	if err != nil {
		log.Fatalf("Open failed %s err %v", f, err)
	}
	re, err := regexp.Compile(p)
	if err != nil {
		log.Fatalf("Compile %v failed err %v", p, err)
	}
	m := re.FindAllString(string(data), -1)
	return len(m)
}

func countPattern(files []string, p string) int {
	n := 0
	for _, f := range files {
		n += countPatternFile(f, p)
	}
	return n
}
