package marshal

import (
	"testing"
	"time"

	"github.com/optiopay/kafka/kafkatest"
)

func MakeTopic(srv *kafkatest.Server, topic string, numPartitions int) {
	for i := 0; i < numPartitions; i++ {
		srv.AddMessages(topic, int32(i))
	}
}

func StartServer() *kafkatest.Server {
	srv := kafkatest.NewServer()
	srv.MustSpawn()
	MakeTopic(srv, MARSHAL_TOPIC, 4)
	MakeTopic(srv, "test1", 1)
	MakeTopic(srv, "test16", 16)
	return srv
}

func TestNewMarshaler(t *testing.T) {
	srv := StartServer()

	m, err := NewMarshaler("cl", "gr", []string{srv.Addr()})
	if err != nil {
		t.Errorf("New failed: %s", err)
	}
	defer m.Terminate()

	ct := m.Partitions(MARSHAL_TOPIC)
	if ct != 4 {
		t.Error("Expected 4 partitions got", ct)
	}

	ct = m.Partitions("test1")
	if ct != 1 {
		t.Error("Expected 1 partitions got", ct)
	}

	ct = m.Partitions("test16")
	if ct != 16 {
		t.Error("Expected 16 partitions got", ct)
	}

	ct = m.Partitions("unknown")
	if ct != 0 {
		t.Error("Expected 0 partitions got", ct)
	}
}

// This is a full integration test of claiming including writing to Kafka via the marshaler
// and waiting for responses
func TestClaimPartitionIntegration(t *testing.T) {
	srv := StartServer()

	m, err := NewMarshaler("cl", "gr", []string{srv.Addr()})
	if err != nil {
		t.Errorf("New failed: %s", err)
	}
	defer m.Terminate()

	resp := make(chan bool)
	go func() {
		resp <- m.ClaimPartition("test1", 0) // true
		resp <- m.ClaimPartition("test1", 0) // true (no-op)
		m.lock.Lock()
		m.clientId = "cl-other"
		m.lock.Unlock()
		resp <- m.ClaimPartition("test1", 0) // false (collission)
		resp <- m.ClaimPartition("test1", 1) // true (new client)
	}()

	select {
	case out := <-resp:
		if !out {
			t.Error("Failed to claim partition")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out claiming partition")
	}

	select {
	case out := <-resp:
		if !out {
			t.Error("Failed to reclaim partition")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out claiming partition")
	}

	select {
	case out := <-resp:
		if out {
			t.Error("Failed to collide with partition claim")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out claiming partition")
	}

	select {
	case out := <-resp:
		if !out {
			t.Error("Failed to claim partition")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out claiming partition")
	}
}
