package marshal

import (
	"math/rand"
	"testing"
	"time"

	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

func Produce(m *Marshaler, topicName string, partID int, msgs ...string) (int64, error) {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}

	return m.producer.Produce(topicName, int32(partID), protos...)
}

func NewTestConsumer(topicName string, behavior ConsumerBehavior) (
	*kafkatest.Server, *Marshaler, *Consumer) {
	srv := StartServer()

	m, err := NewMarshaler("cl", "gr", []string{srv.Addr()})
	if err != nil {
		return nil, nil, nil
	}

	return srv, m, &Consumer{
		marshal:    m,
		topic:      topicName,
		partitions: m.Partitions(topicName),
		behavior:   behavior,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[int]*consumerClaim),
	}
}

func TestNewConsumer(t *testing.T) {
	srv := StartServer()

	m, err := NewMarshaler("cl", "gr", []string{srv.Addr()})
	if err != nil {
		t.Errorf("New failed: %s", err)
	}
	defer m.Terminate()

	c, err := NewConsumer(m, "test1", CbAggressive)
	if err != nil {
		t.Errorf("Failed to create: %s", err)
	}
	defer c.Terminate()

	// A new consumer will immediately start to claim partitions, so let's give it a
	// second and then see if it has
	time.Sleep(500 * time.Millisecond)
	if m.GetPartitionClaim("test1", 0).LastHeartbeat == 0 {
		t.Error("Partition didn't get claimed")
	}

	// Test basic consumption
	_, err = Produce(m, "test1", 0, "m1", "m2", "m3")
	if err != nil {
		t.Errorf("Failed to produce: %s", err)
	}

	m1 := c.Consume()
	if string(m1) != "m1" {
		t.Errorf("Expected m1, got: %s", m1)
	}

	m2 := c.Consume()
	if string(m2) != "m2" {
		t.Errorf("Expected m2, got: %s", m2)
	}

	m3 := c.Consume()
	if string(m3) != "m3" {
		t.Errorf("Expected m3, got: %s", m3)
	}

	// TODO: flesh out test, can create a second consumer and then see if it gets any
	// partitions, etc.
	// lots of things can be tested.
}

func TestTryClaimPartition(t *testing.T) {
	_, m, c := NewTestConsumer("test1", CbAggressive)
	defer c.Terminate()
	defer m.Terminate()

	if !c.tryClaimPartition(0) {
		t.Error("Failed to claim partition 0")
	}
	if c.tryClaimPartition(0) {
		t.Error("Succeeded to claim partition 0")
	}
}

func TestOffsetUpdates(t *testing.T) {
	_, m, c := NewTestConsumer("test1", CbAggressive)
	defer c.Terminate()
	defer m.Terminate()

	if !c.tryClaimPartition(0) {
		t.Error("Failed to claim partition 0")
	}

	err := c.updateOffsets()
	if err != nil {
		t.Errorf("Failed to update offests: %s", err)
	}

	o, err := Produce(m, "test1", 0, "m1", "m2", "m3")
	if err != nil {
		t.Errorf("Failed to produce: %s", err)
	}
	if o != 2 {
		t.Errorf("Expected offset 2, got %d", o)
	}

	err = c.updateOffsets()
	if err != nil {
		t.Errorf("Failed to update offests: %s", err)
	}

	if c.claims[0].offsetLatest != 3 {
		t.Errorf("Latest offest should be 3, got %d", c.claims[0].offsetLatest)
	}
}

func TestAggressiveClaim(t *testing.T) {
	_, m, c := NewTestConsumer("test16", CbAggressive)
	defer c.Terminate()
	defer m.Terminate()

	c.claimPartitions()
	if c.GetCurrentLoad() != 16 {
		t.Error("Did not claim 16 partitions")
	}
}

func TestBalancedClaim(t *testing.T) {
	_, m, c := NewTestConsumer("test16", CbBalanced)
	defer c.Terminate()
	defer m.Terminate()

	c.claimPartitions()
	if c.GetCurrentLoad() != 1 {
		t.Error("Did not claim 1 partition")
	}
}
