package main

import (
	"github.com/performancecopilot/speed"
	"strconv"
 	"hash/fnv"
)

type ClientRegistry struct {
	PCPClients map[uint32]*speed.PCPClient
	CheckCollisions bool
	CollisionCounter int
}

var (
	metricRegistry map[uint32]string
)

const MAX_COLLISIONS_REPORTED = 2000

func NewClientRegistry(check bool) *ClientRegistry {
	speed.EraseFileOnStop = false
	metricRegistry = make(map[uint32]string)
	return &ClientRegistry{make(map[uint32]*speed.PCPClient), check, 0}
}

// Hash mechanism used in Speed library.
// Returns 6 bits for client/cluster and 10 bits for metric.
// This generates maximum of 64 clusters.
func hash610(s string) (uint32, uint32, uint32) {
  h := fnv.New32a()
  _, err := h.Write([]byte(s))
  if err != nil {
    panic(err)
  }
  hash := h.Sum32()
  x06 := hash & 0xFC00
  x10 := hash & 0x03FF
  return (x06 >> 8), x10, (hash & 0xFFFF)
}

func clusterName(hash uint32) string {
	return "statsd_" + strconv.FormatUint(uint64(hash), 16)
}

func (registry *ClientRegistry) FindClientForMetric(metric string) (*speed.PCPClient, error) {
	clusterHash, _, hash32  := hash610(metric)
	client, ok := registry.PCPClients[clusterHash]
	if ok {
		return client, nil
	} else {
		// create new client
		clientName := clusterName(clusterHash)
		DebugLog.Printf("Creating client named '%s'\n", clientName)
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
		registry.PCPClients[clusterHash] = client
		if registry.CheckCollisions && registry.CollisionCounter < MAX_COLLISIONS_REPORTED {
			// check for collisions
			existingName, ok := metricRegistry[hash32]
			if ok {
				if metric != existingName {
					DebugLog.Printf("Hash collision: %s vs %s (%X)\n", existingName, metric, hash32)
					if registry.CollisionCounter == MAX_COLLISIONS_REPORTED {
						DebugLog.Printf("Too many collisions reported, disabled collision logging\n")
					}
				}
			} else {
				metricRegistry[hash32] = metric
			}
		}
		return client, nil
	}
}

func (registry *ClientRegistry) Stop() {
	speed.EraseFileOnStop = true
	for clusterId, client := range registry.PCPClients {
		DebugLog.Printf("Stopping client %s", clusterName(clusterId))
		client.MustStop()
	}
}
