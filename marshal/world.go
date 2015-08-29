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

	"github.com/optiopay/kafka"
)

// worldState is the main structure where we store informatio about the state of all of the
// consumers and partitions.
type worldState struct {
	quit     *int32
	clientId string
	groupId  string

	lock   sync.RWMutex
	topics map[string]*topicState

	kafka *kafka.Broker
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

func (w *worldState) IsClaimed(topic string, partId int) bool {
	return false
}

func (w *worldState) ClaimPartition(topic string, partId int) (bool, error) {
	return false, nil
}
