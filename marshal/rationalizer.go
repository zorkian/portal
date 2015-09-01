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
	topic := w.getTopicState(msg.Topic, msg.PartId)

	topic.lock.Lock()
	defer topic.lock.Unlock()

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
	topic := w.getTopicState(msg.Topic, msg.PartId)

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

// handleClaim is called whenever we see a ClaimPartition message.
func (w *MarshalState) handleClaim(msg *msgClaimingPartition) {
	topic := w.getTopicState(msg.Topic, msg.PartId)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Send message to all pending consumers then clear the list (it is a violation of the
	// protocol to send two responses)
	fireEvents := func(evt bool) {
		for _, out := range topic.partitions[msg.PartId].pendingClaims {
			out <- evt
		}
		topic.partitions[msg.PartId].pendingClaims = nil
	}

	// Claim logic: if the claim is for an already claimed partition, we can end now and decide
	// whether or not to fire.
	if topic.partitions[msg.PartId].isClaimed(w.ts) {
		// The ClaimPartition message needs to be from us, or we should just return
		if msg.ClientId == w.clientId && msg.GroupId == w.groupId {
			// Now determine if we own the partition claim and let us know whether we do
			// or not
			if topic.partitions[msg.PartId].ClientId == w.clientId &&
				topic.partitions[msg.PartId].GroupId == w.groupId {
				fireEvents(true)
			} else {
				fireEvents(false)
			}
		}
		return
	}

	// At this point, the partition is unclaimed, which means we know we have the first
	// ClaimPartition message. As soon as we get it, we fill in the structure which makes
	// us think it's claimed (it is).
	topic.partitions[msg.PartId].ClientId = msg.ClientId
	topic.partitions[msg.PartId].GroupId = msg.GroupId
	topic.partitions[msg.PartId].LastOffset = 0 // not present in this message, reset.
	topic.partitions[msg.PartId].LastHeartbeat = int64(msg.Time)

	// Now we can advise that this partition has been handled
	if msg.ClientId == w.clientId && msg.GroupId == w.groupId {
		fireEvents(true)
	} else {
		fireEvents(true)
	}
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
			w.handleClaim(msg.(*msgClaimingPartition))
		case msgTypeReleasingPartition:
			w.releaseClaim(msg.(*msgReleasingPartition))
		case msgTypeClaimingMessages:
			// TODO: Implement.
		}
	}
}
