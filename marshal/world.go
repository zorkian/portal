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
	"github.com/optiopay/kafka/proto"
)

// MarshalState is the main structure where we store information about the state of all of the
// consumers and partitions.
type MarshalState struct {
	quit     *int32
	clientId string
	groupId  string

	lock   sync.RWMutex
	topics map[string]int
	groups map[string]map[string]*topicState

	kafka         *kafka.Broker
	kafkaProducer kafka.Producer

	// This is for testing only. When this is non-zero, the rationalizer will answer
	// queries based on THIS time instead of the current, actual time.
	ts int64
}

// getTopicState returns a topicState and possibly creates it and the partition state within
// the MarshalState.
func (w *MarshalState) getTopicState(topicName string, partId int) *topicState {
	w.lock.Lock()
	defer w.lock.Unlock()

	group, ok := w.groups[w.groupId]
	if !ok {
		group = make(map[string]*topicState)
		w.groups[w.groupId] = group
	}

	topic, ok := group[topicName]
	if !ok {
		topic = &topicState{
			partitions: make([]PartitionClaim, partId+1),
		}
		group[topicName] = topic
	}

	// They might be referring to a partition we don't know about, maybe extend it
	if len(topic.partitions) < partId+1 {
		for i := len(topic.partitions); i <= partId; i++ {
			topic.partitions = append(topic.partitions, PartitionClaim{})
		}
	}

	return topic
}

// Topics returns the list of known topics.
func (w *MarshalState) Topics() []string {
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
func (w *MarshalState) Partitions(topicName string) int {
	w.lock.RLock()
	defer w.lock.RUnlock()

	count, _ := w.topics[topicName]
	return count
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (w *MarshalState) Terminate() {
	atomic.StoreInt32(w.quit, 1)
}

// IsClaimed returns the current status on whether or not a partition is claimed by any other
// consumer in our group (including ourselves). A topic/partition that does not exist is
// considered to be unclaimed.
func (w *MarshalState) IsClaimed(topicName string, partId int) bool {
	// The contract of this method is that if it returns something and the heartbeat is
	// non-zero, the partition is claimed.
	claim := w.GetPartitionClaim(topicName, partId)
	return claim.LastHeartbeat > 0
}

// GetPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently claiming this partition. This is a copy of the
// claim structure, so changing it cannot change the world state.
func (w *MarshalState) GetPartitionClaim(topicName string, partId int) PartitionClaim {
	topic := w.getTopicState(topicName, partId)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	if topic.partitions[partId].isClaimed(w.ts) {
		return topic.partitions[partId] // copy.
	}
	return PartitionClaim{}
}

// ClaimPartition is how you can actually claim a partition. If you call this, Marshal will
// attempt to claim the partition on your behalf. This is the low level function, you probably
// want to use a MarshaledConsumer. Returns a bool on whether or not the claim succeeded and
// whether you can continue.
func (w *MarshalState) ClaimPartition(topicName string, partId int) bool {
	topic := w.getTopicState(topicName, partId)

	// Unlock is later, since this function might take a while
	topic.lock.Lock()

	// If the topic is already claimed, we can short circuit the decision process
	if topic.partitions[partId].isClaimed(w.ts) {
		defer topic.lock.Unlock()
		if topic.partitions[partId].GroupId == w.groupId &&
			topic.partitions[partId].ClientId == w.clientId {
			return true
		} else {
			log.Warning("Attempt to claim already claimed partition.")
			return false
		}
	}

	// Make a channel for results, append it to the list so we hear about claims
	out := make(chan bool, 1)
	topic.partitions[partId].pendingClaims = append(
		topic.partitions[partId].pendingClaims, out)
	topic.lock.Unlock()

	// Produce message to kafka
	// TODO: Make this work on more than just partition 0. Hash by the topic/partition we're
	// trying to claim, or something...
	cl := &msgClaimingPartition{
		msgBase: msgBase{
			Time:     int(time.Now().Unix()),
			ClientId: w.clientId,
			GroupId:  w.groupId,
			Topic:    topicName,
			PartId:   partId,
		},
	}
	_, err := w.kafkaProducer.Produce(MARSHAL_TOPIC, 0,
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		// If we failed to produce, this is probably serious so we should undo the work
		// we did and then return failure
		log.Error("Failed to produce to Kafka: %s", err)
		return false
	}

	// Finally wait and return the result. The rationalizer should see the above message
	// and know it was from us, and will be able to know if we won or not.
	return <-out
}
