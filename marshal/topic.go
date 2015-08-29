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
	lock       sync.RWMutex
	partitions []partitionState
}

// partitionState contains information about a given partition.
type partitionState struct {
}
