// Package apps provides a static registry of MapReduce applications.
//
// The stock 6.5840 lab loads each application's Map/Reduce functions from a
// compiled plugin (.so) via plugin.Open. Go's -buildmode=plugin is not
// supported on Windows, so this Windows-native port compiles every app into
// the binary and looks them up by name instead.
//
// Load accepts either a plain name ("wc") or a legacy plugin path
// ("../../mrapps/wc.so"); the path's base name minus the ".so" suffix is used
// as the lookup key, so the unmodified test harness (which still passes
// ".so" paths) keeps working.
package apps

import (
	"log"
	"path/filepath"
	"strings"

	"6.5840/mr"

	"6.5840/mrapps/crash"
	"6.5840/mrapps/earlyexit"
	"6.5840/mrapps/indexer"
	"6.5840/mrapps/jobcount"
	"6.5840/mrapps/mtiming"
	"6.5840/mrapps/nocrash"
	"6.5840/mrapps/rtiming"
	"6.5840/mrapps/wc"
)

// MapFunc and ReduceFunc match the signatures used throughout the lab.
type MapFunc func(string, string) []mr.KeyValue
type ReduceFunc func(string, []string) string

type app struct {
	mapf    MapFunc
	reducef ReduceFunc
}

var registry = map[string]app{
	"wc":         {wc.Map, wc.Reduce},
	"indexer":    {indexer.Map, indexer.Reduce},
	"mtiming":    {mtiming.Map, mtiming.Reduce},
	"rtiming":    {rtiming.Map, rtiming.Reduce},
	"jobcount":   {jobcount.Map, jobcount.Reduce},
	"early_exit": {earlyexit.Map, earlyexit.Reduce},
	"crash":      {crash.Map, crash.Reduce},
	"nocrash":    {nocrash.Map, nocrash.Reduce},
}

// Load returns the Map and Reduce functions for the named application.
func Load(name string) (MapFunc, ReduceFunc) {
	key := strings.TrimSuffix(filepath.Base(name), ".so")
	a, ok := registry[key]
	if !ok {
		log.Fatalf("unknown mrapp %q (from %q)", key, name)
	}
	return a.mapf, a.reducef
}
