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
func (w *worldState) kafkaConsumerChannel(partId int) <-chan message {
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

// rationalize is a goroutine that constantly consumes from a given partition of the marshal
// topic and makes changes to the world state whenever something happens.
func (w *worldState) rationalize(partId int, in <-chan message) { // Might be in over my head.
	for {
		msg, ok := <-in
		if !ok {
			log.Info("rationalize[%d]: channel closed.", partId)
			return
		}

		log.Info("rationalize[%d]: received: %s", partId, msg.Encode())
	}
}
