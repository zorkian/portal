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
	partitions []partitionState
}

// partitionState contains information about a given partition. NB: You must have the topic lock
// to operate on a partition within the topic.
type partitionState struct {
	lastHeartbeat int64
	lastOffset    int
	clientId      string
	groupId       string
}
