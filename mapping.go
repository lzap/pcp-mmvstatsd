package main

import (
 	"encoding/json"
 	"os"
 	"io/ioutil"
)

type Metrics struct {
	Metrics []Metric `json:"metrics"`
}

type Metric struct {
	Name   string `json:"name"`
	Match   string `json:"match"`
	Description  string `json:"description"`
	ID    int    `json:"id"`
}

var metrics Metrics

func NewFromFile(file string) error {
	jsonFile, err := os.Open(file)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	// load JSON
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(byteValue, &metrics)
	if err != nil {
		return err
	}

	// generate the regexp
	for _, m := range metrics.Metrics {
		TraceLog.Println("Loaded metric name " + m.Name)
	}
	return nil
}
