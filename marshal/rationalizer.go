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

// rationalize is a goroutine that constantly consumes from a given partition of the marshal
// topic and makes changes to the world state whenever something happens.
func (w *worldState) rationalize(partId int) { // Might be in over my head.
	// TODO: Technically we don't have to start at the beginning, we just need to start back
	// a couple heartbeat intervals to get a full state of the world. But this is easiest
	// for right now...
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
			log.Info("rationalize[%d]: terminating.", partId)
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
			log.Error("rationalize[%d]: %s", partId, err)
			continue
		}

		log.Info("Got message %d: [%s]", msg.Type(), msg.Encode())
	}
}
