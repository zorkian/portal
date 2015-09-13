/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

// ConsumerBehavior is the broad category of behaviors that encapsulate how the Consumer
// will handle claiming/releasing partitions.
type ConsumerBehavior int

const (
	// CbAggressive specifies that the consumer should attempt to claim all unclaimed
	// partitions immediately. This is appropriate in low QPS situations where you are
	// mainly using this library to ensure failover to standby consumers.
	CbAggressive ConsumerBehavior = iota

	// CbBalanced ramps up more slowly than CbAggressive, but is more appropriate in
	// high QPS situations where you know that a single Consumer will never be able to
	// handle the entire topic's traffic.
	CbBalanced = iota
)

// consumerClaim is our internal tracking structure about a partition.
type consumerClaim struct {
	topic         string
	partID        int
	claimed       *int32
	lastHeartbeat int64
	consumer      kafka.Consumer
	messages      chan *proto.Message

	// offsetCurrent is the present position of our consumer. This is what gets
	// reported in the heartbeats when needed.
	offsetCurrent int64

	// offsetEarliest is the "oldest" offset available in the partition.
	offsetEarliest int64

	// offsetLatest is the "newest" offset available in the partition.
	offsetLatest int64
}

// isClaimed is a helper function, returns true/false on whether or not the partition
// claim is active. (Thread safe.)
func (c *consumerClaim) isClaimed() bool {
	return atomic.LoadInt32(c.claimed) == 1
}

// messagePump continuously pulls message from Kafka for this partition and makes them
// available for consumption.
func (c *consumerClaim) messagePump() {
	for {
		if !c.isClaimed() {
			log.Infof("%s:%d no longer claimed, pump exiting.", c.topic, c.partID)
			return
		}

		msg, err := c.consumer.Consume()
		if err != nil {
			log.Errorf("%s:%d error consuming: %s", c.topic, c.partID, err)
			// TODO: What can we do here? Probably if we got an error it's just
			// a transient thing, so let's have some backoff here?
			continue
		}

		c.messages <- msg
	}
}

// Consumer allows you to safely consume data from a given topic in such a way that you
// don't need to worry about partitions and can safely split the load across as many
// processes as might be consuming from this topic. However, you should ONLY create one
// Consumer per topic in your application!
type Consumer struct {
	marshal    *Marshaler
	topic      string
	partitions int
	rand       *rand.Rand
	claims     map[int]*consumerClaim
	lock       sync.RWMutex
	behavior   ConsumerBehavior
}

// NewConsumer instantiates a consumer object for a given topic. You must create a
// separate consumer for every individual topic that you want to consume from. Please
// see the documentation on ConsumerBehavior.
func NewConsumer(marshal *Marshaler, topicName string,
	behavior ConsumerBehavior) (*Consumer, error) {

	if marshal == nil {
		return nil, errors.New("Must provide a marshaler")
	}

	consumer := &Consumer{
		marshal:    marshal,
		topic:      topicName,
		partitions: marshal.Partitions(topicName),
		behavior:   behavior,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[int]*consumerClaim),
	}
	go consumer.manageClaims()

	return consumer, nil
}

// updateOffsets will update the offsets of any partitions that we claim.
func (c *Consumer) updateOffsets() error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for partID, claim := range c.claims {
		oEarly, oLate, _, err := c.marshal.GetPartitionOffsets(c.topic, partID)
		if err != nil {
			log.Errorf("Failed to get offsets for %s:%d: %s", c.topic, partID, err)
			return err
		}

		// DO NOT update current, but we can update early/late
		claim.offsetEarliest = oEarly
		claim.offsetLatest = oLate
	}
	return nil
}

// tryClaimPartition attempts to claim a partition and make it available in the consumption
// flow. If this is called a second time on a partition we already own, it will return
// false. Returns true only if the partition was never claimed and we succeeded in
// claiming it.
func (c *Consumer) tryClaimPartition(partID int) bool {
	// Partition unclaimed by us, see if it's claimed by anybody
	// TODO: This is where we probably want to insert "claim reassertion" logic? I.e.,
	// if the clientid/groupid are the same, here is where we would promote the claim
	// back into our own structure so the consumer would just pick up where it left off
	currentClaim := c.marshal.GetPartitionClaim(c.topic, partID)
	if currentClaim.LastHeartbeat > 0 {
		return false
	}

	// Get all available offset information
	oEarly, oLate, oCur, err := c.marshal.GetPartitionOffsets(c.topic, partID)
	if err != nil {
		log.Errorf("Failed to get offsets for %s:%d: %s", c.topic, partID, err)
		return false
	}

	// Set up internal claim structure we'll track things in
	claim := &consumerClaim{
		topic:          c.topic,
		partID:         partID,
		claimed:        new(int32),
		offsetEarliest: oEarly,
		offsetLatest:   oLate,
		offsetCurrent:  oCur,
		messages:       make(chan *proto.Message, 100),
	}
	atomic.StoreInt32(claim.claimed, 1)

	// Now try to actually claim it, this can block a while
	log.Infof("Consumer attempting to claim: %s:%d", c.topic, partID)
	if !c.marshal.ClaimPartition(c.topic, partID) {
		log.Infof("Consumer failed to claim: %s:%d", c.topic, partID)
		return false
	}

	// Of course, if the current offset is greater than the earliest, we must reset
	// to the earliest known
	if claim.offsetCurrent < claim.offsetEarliest {
		log.Warningf("Consumer fast-forwarding %s:%d: from %d to %d",
			c.topic, partID, claim.offsetCurrent, claim.offsetEarliest)
		claim.offsetCurrent = claim.offsetEarliest
	}

	// Since it's claimed, we now want to heartbeat with the last seen offset
	err = c.marshal.Heartbeat(c.topic, partID, claim.offsetCurrent)
	if err != nil {
		log.Errorf("Consumer failed to heartbeat: %s:%d", c.topic, partID)
	}

	// Set up Kafka consumer
	consumerConf := kafka.NewConsumerConf(c.topic, int32(partID))
	consumerConf.StartOffset = claim.offsetCurrent
	kafkaConsumer, err := c.marshal.kafka.Consumer(consumerConf)
	if err != nil {
		log.Errorf("Consumer failed to create Kafka Consumer: %s:%d got %s",
			c.topic, partID, err)
		// TODO: There is an optimization here where we could release the partition.
		// As it stands, we're not doing anything,
		return false
	}
	claim.consumer = kafkaConsumer

	// Now start consuming from this partition (as long as we haven't terminated)
	go claim.messagePump()

	// Totally done, update our internal structures
	log.Infof("Consumer claimed: %s:%d at offset %d (is %d behind)",
		c.topic, partID, claim.offsetCurrent, claim.offsetLatest)

	// Finally overwrite our structure pointer (state is committed to ourselves)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.claims[partID] = claim
	return true
}

// claimPartitions actually attempts to claim partitions. If the current consumer is
// set on aggressive, this will try to claim ALL partitions that are free.
func (c *Consumer) claimPartitions() {
	offset := rand.Intn(c.partitions)
	for i := 0; i < c.partitions; i++ {
		partID := (i + offset) % c.partitions

		c.lock.RLock()
		if _, ok := c.claims[partID]; ok {
			if c.claims[partID].isClaimed() {
				c.lock.RUnlock()
				continue
			}
		}
		c.lock.RUnlock()

		if !c.tryClaimPartition(partID) {
			continue
		}

		// Balanced mode means we abort our claim now, since we've got one, whereas
		// aggressive claims as many as it can
		if c.behavior == CbBalanced {
			break
		}
	}
}

// manageClaims is our internal state machine that handles partitions and claiming new
// ones (or releasing ones).
func (c *Consumer) manageClaims() {
	for {
		// Step 1: For partitions we have, if they're behind increment the behind count
		// so we can possibly release them.
		// Determine our overall health (i.e. how far behind we are, and whether or not
		// we're falling more behind)

		// Maintain heartbeats on claims we're still happy about

		// Handle unclaim events (if we didn't heartbeat or something)

		// At this point, we don't need to engage in any load shedding behavior, so let's
		// see if there are any partitions out there we can claim
		c.claimPartitions()

		// Now sleep a bit so we don't pound things
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}
}

// Terminate instructs the consumer to release its locks. This will allow other consumers
// to begin consuming. (If you do not call this method before exiting, things will still
// work, but more slowly.)
func (c *Consumer) Terminate() {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for partID, claim := range c.claims {
		if claim.isClaimed() {
			err := c.marshal.ReleasePartition(c.topic, partID, claim.offsetCurrent)
			if err == nil {
				log.Infof("Consumer termination: released %s:%d at %d",
					c.topic, partID, claim.offsetCurrent)
			} else {
				log.Errorf("Consumer termination: failed to release %s:%d: %s",
					c.topic, partID, err)
			}
		}
	}
}

// GetCurrentLag returns the number of messages that this consumer is lagging by. Note that
// this value can be unstable in the beginning of a run, as we might not have claimed all of
// partitions we will end up claiming, or we might have overclaimed and need to back off.
// Ideally this will settle towards 0. If it continues to rise, that implies there isn't
// enough consumer capacity.
func (c *Consumer) GetCurrentLag() int {
	return -1
}

// GetCurrentLoad returns a number representing the "load" of this consumer. Think of this
// like a load average in Unix systems: the numbers are kind of related to how much work
// the system is doing, but by itself they don't tell you much.
func (c *Consumer) GetCurrentLoad() int {
	claimed := 0
	for _, claim := range c.claims {
		if claim.isClaimed() {
			claimed++
		}
	}
	return claimed
}

// Consume returns the next available message from the topic. If no messages are available,
// it will block until one is.
func (c *Consumer) Consume() []byte {
	// TODO: This is almost certainly a slow implementation as we have to scan everything
	// every time.
	// TODO: This implementation also can lead to queue starvation since we start at the
	// front every time.
	for {
		var msg *proto.Message

		c.lock.RLock()
		for _, claim := range c.claims {
			if !claim.isClaimed() {
				continue
			}

			select {
			case msg = <-claim.messages:
				break
			default:
				// Do nothing.
			}
		}
		c.lock.RUnlock()

		if msg == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Once we're consuming a message we need to update our data structure as well
		// as possibly make a heartbeat for this partition so the world knows we're
		// still actively consuming
		// TODO: There is a trap here, this is actually probably a bad design in that
		// the consumer has its own idea of "claim" which in theory mirrors what the
		// marshaler has... but there's no guarantee

		return msg.Value
	}
}
