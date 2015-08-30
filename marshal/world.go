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
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
)

// worldState is the main structure where we store information about the state of all of the
// consumers and partitions.
type worldState struct {
	quit     *int32
	clientId string
	groupId  string

	lock   sync.RWMutex
	topics map[string]*topicState

	kafka *kafka.Broker

	// This is for testing only. When this is non-zero, the rationalizer will answer
	// queries based on THIS time instead of the current, actual time.
	ts int64
}

// Topics returns the list of known topics.
func (w *worldState) Topics() []string {
	w.lock.RLock()
	defer w.lock.RUnlock()

	topics := make([]string, 0, len(w.topics))
	for topic, _ := range w.topics {
		topics = append(topics, topic)
	}
	return topics
}

// Partitions returns the count of how many partitions are in a given topic. Returns 0 if a
// topic is unknown.
func (w *worldState) Partitions(topicName string) int {
	w.lock.RLock()
	defer w.lock.RUnlock()

	topic, ok := w.topics[topicName]
	if !ok {
		return 0
	}

	topic.lock.RLock()
	defer topic.lock.RUnlock()
	return len(topic.partitions)
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (w *worldState) Terminate() {
	atomic.StoreInt32(w.quit, 1)
}

// IsClaimed returns the current status on whether or not a partition is claimed by any other
// consumer (including ourselves). A topic/partition that does not exist is considered to be
// unclaimed.
func (w *worldState) IsClaimed(topicName string, partId int) bool {
	w.lock.RLock()
	defer w.lock.RUnlock()

	topic, ok := w.topics[topicName]
	if !ok {
		return false
	}
	topic.lock.RLock()
	defer topic.lock.RUnlock()

	if partId > len(topic.partitions) {
		return false
	}

	// Calculate claim validity based on the delta between NOW and lastHeartbeat:
	//
	// delta = 0 .. HEARTBEAT_INTERVAL: claim good.
	//         HEARTBEAT_INTERVAL .. 2*HEARTBEAT_INTERVAL-1: claim good.
	//         >2xHEARTBEAT_INTERVAL: claim invalid.
	//
	// This means that the worst case for a "dead consumer" that has failed to heartbeat
	// is that a partition will be idle for twice the heartbeat interval.
	//

	now := w.ts
	if now == 0 {
		now = time.Now().Unix()
	}

	delta := now - topic.partitions[partId].lastHeartbeat
	switch {
	case 0 <= delta && delta <= HEARTBEAT_INTERVAL:
		return true
	case HEARTBEAT_INTERVAL < delta && delta < 2*HEARTBEAT_INTERVAL:
		log.Warning("Claim on %s:%d is aging: %d seconds.", topicName, partId, delta)
		return true
	default:
		return false
	}
}

func (w *worldState) ClaimPartition(topicName string, partId int) (bool, error) {
	return false, nil
}
