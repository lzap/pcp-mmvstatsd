package main

import (
	"log"
	"os"
	"io/ioutil"
)

var (
    TraceLog = log.New(ioutil.Discard, "", 0)
    DebugLog = log.New(ioutil.Discard, "", 0)
    VerboseLog = log.New(ioutil.Discard, "", 0)
    ErrorLog = log.New(os.Stderr, "ERROR: ", 0)
)

func EnableLoggers(on bool, loggers ... *log.Logger) {
	if on {
		for _, logger := range loggers {
			logger.SetOutput(os.Stdout)
		}
	}
}
