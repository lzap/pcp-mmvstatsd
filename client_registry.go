package main

import (
	"github.com/performancecopilot/speed"
	"strconv"
 	"hash/fnv"
)

type ClientRegistry map[uint32]*speed.PCPClient

var (
	metricRegistry map[uint32]string
)

func NewClientRegistry() ClientRegistry {
	speed.EraseFileOnStop = false
	metricRegistry = make(map[uint32]string)
	return make(ClientRegistry)
}

// Hash mechanism used in Speed library.
// Returns 6 bits for client/cluster and 10 bits for metric.
// This generates maximum of 64 clusters.
func hash610(s string) (uint32, uint32) {
  h := fnv.New32a()
  _, err := h.Write([]byte(s))
  if err != nil {
    panic(err)
  }
  hash := h.Sum32()
  x06 := hash & 0xFC00
  x10 := hash & 0x03FF
  return x06, x10
}

func clusterName(hash uint32) string {
	return "statsd_" + strconv.FormatUint(uint64(hash >> 8), 16)
}

func (registry ClientRegistry) FindClientForMetric(metric string) (*speed.PCPClient, error) {
	clusterHash, metricHash  := hash610(metric)
	client, ok := registry[clusterHash]
	if ok {
		return client, nil
	} else {
		// create new client
		clientName := clusterName(clusterHash)
		DebugLog.Printf("Creating new cluster/client named '%s'\n", clientName)
		client, err := speed.NewPCPClient(clientName)
		if err != nil {
			return nil, err
		}
		err = client.SetFlag(speed.NoPrefixFlag | speed.ProcessFlag)
		if err != nil {
			return nil, err
		}
		err = client.Start()
		if err != nil {
			return nil, err
		}
		registry[clusterHash] = client
		// check for collisions
		existingName, ok := metricRegistry[metricHash]
		if ok {
			if metric != existingName {
				TraceLog.Printf("Possible collision: '%s' vs '%s'", existingName, metric)
			}
		} else {
			metricRegistry[metricHash] = metric
		}
		return client, nil
	}
}

func (registry ClientRegistry) Stop() {
	speed.EraseFileOnStop = true
	for clusterId, client := range registry {
		DebugLog.Printf("Stopping client %s", clusterName(clusterId))
		client.MustStop()
	}
}
