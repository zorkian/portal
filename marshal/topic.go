/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"sync"
)

// topicState contains information about a given topic.
type topicState struct {
	// This lock also protects the contenst of the partitions.
	lock       sync.RWMutex
	partitions []PartitionClaim
}

// PartitionClaim contains claim information about a given partition.
type PartitionClaim struct {
	LastHeartbeat int64
	LastOffset    int
	ClientId      string
	GroupId       string
}
