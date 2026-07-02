package shardgrp

import "log"

// Debugging
const Debug = false

func DPrintln(a ...interface{}) {
	if Debug {
		log.Println(a...)
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
