/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
)

// kafkaConsumerChannel creates a consumer that continuously attempts to consume messages from
// Kafka for the given partition.
func (w *MarshalState) kafkaConsumerChannel(partId int) <-chan message {
	out := make(chan message, 1000)
	go func() {
		// TODO: Technically we don't have to start at the beginning, we just need to start back
		// a couple heartbeat intervals to get a full state of the world. But this is easiest
		// for right now...
		// TODO: Think about the above. Is it actually true?
		consumerConf := kafka.NewConsumerConf(MARSHAL_TOPIC, int32(partId))
		consumerConf.StartOffset = kafka.StartOffsetOldest
		consumerConf.RequestTimeout = 1 * time.Second

		consumer, err := w.kafka.Consumer(consumerConf)
		if err != nil {
			// Unfortunately this is a fatal error, as without being able to consume this partition
			// we can't effectively rationalize.
			log.Fatalf("rationalize[%d]: Failed to create consumer: %s", partId, err)
		}

		// Consume messages forever, or until told to quit.
		for {
			if atomic.LoadInt32(w.quit) == 1 {
				log.Debug("rationalize[%d]: terminating.", partId)
				close(out)
				return
			}

			msgb, err := consumer.Consume()
			if err != nil {
				// The internal consumer will do a number of retries. If we get an error here,
				// for now let's consider it fatal.
				log.Fatalf("rationalize[%d]: failed to consume: %s", partId, err)
			}

			msg, err := Decode(msgb.Value)
			if err != nil {
				// Invalid message in the stream. This should never happen, but if it does, just
				// continue on.
				// TODO: We should probably think about this. If we end up in a situation where
				// one version of this software has a bug that writes invalid messages, it could
				// be doing things we don't anticipate. Of course, crashing all consumers
				// reading that partition is also bad.
				log.Error("rationalize[%d]: %s", partId, err)
				continue
			}

			log.Debug("Got message at offset %d: [%s]", msgb.Offset, msg.Encode())
			out <- msg
		}
	}()
	return out
}

// updateClaim is called whenever we need to adjust a claim structure.
func (w *MarshalState) updateClaim(msg *msgHeartbeat) {
	w.lock.Lock()
	defer w.lock.Unlock()

	group, ok := w.groups[msg.GroupId]
	if !ok {
		group = make(map[string]*topicState)
		w.groups[msg.GroupId] = group
	}

	topic, ok := group[msg.Topic]
	if !ok {
		topic = &topicState{
			partitions: make([]PartitionClaim, msg.PartId+1),
		}
		group[msg.Topic] = topic
	}

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// They might be referring to a partition we don't know about, so let's extend our data
	// structure if so.
	if len(topic.partitions) < msg.PartId+1 {
		for i := len(topic.partitions); i <= msg.PartId; i++ {
			topic.partitions = append(topic.partitions, PartitionClaim{})
		}
	}

	// Note that a heartbeat will just set the claim structure. It's not valid to heartbeat
	// for something you don't own (which is why we have ClaimPartition as a separate
	// message), so we can only assume it's valid.

	topic.partitions[msg.PartId].ClientId = msg.ClientId
	topic.partitions[msg.PartId].GroupId = msg.GroupId
	topic.partitions[msg.PartId].LastOffset = msg.LastOffset
	topic.partitions[msg.PartId].LastHeartbeat = int64(msg.Time)
}

// releaseClaim is called whenever someone has released their claim on a partition.
func (w *MarshalState) releaseClaim(msg *msgReleasingPartition) {
	w.lock.Lock()
	defer w.lock.Unlock()

	group, ok := w.groups[msg.GroupId]
	if !ok {
		log.Warning("Release received for unknown group.")
		return
	}

	topic, ok := group[msg.Topic]
	if !ok {
		// In this particular case, we didn't know about the topic so we didn't know it
		// was claimed anyway. We can just return.
		log.Warning("Release received for unknown topic/partition. Bad client?")
		return
	}

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// The partition must be claimed by the person releasing it
	if topic.partitions[msg.PartId].ClientId != msg.ClientId ||
		topic.partitions[msg.PartId].GroupId != msg.GroupId {
		log.Warning("ReleasePartition message from client that doesn't own it. Dropping.")
		return
	}

	// Record the offset they told us they last processed, and then set the heartbeat to 0
	// which means this is no longer claimed
	topic.partitions[msg.PartId].LastOffset = msg.LastOffset
	topic.partitions[msg.PartId].LastHeartbeat = 0
}

// rationalize is a goroutine that constantly consumes from a given partition of the marshal
// topic and makes changes to the world state whenever something happens.
func (w *MarshalState) rationalize(partId int, in <-chan message) { // Might be in over my head.
	for {
		msg, ok := <-in
		if !ok {
			log.Debug("rationalize[%d]: channel closed.", partId)
			return
		}
		log.Debug("rationalize[%d]: %s", partId, msg.Encode())

		switch msg.Type() {
		case msgTypeHeartbeat:
			w.updateClaim(msg.(*msgHeartbeat))
		case msgTypeClaimingPartition:
			// TODO: Implement.
		case msgTypeReleasingPartition:
			w.releaseClaim(msg.(*msgReleasingPartition))
		case msgTypeClaimingMessages:
			// TODO: Implement.
		}
	}
}
