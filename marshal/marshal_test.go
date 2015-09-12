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
	MakeTopic(srv, MarshalTopic, 4)
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

	ct := m.Partitions(MarshalTopic)
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
		m.clientID = "cl-other"
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

// This is a full integration test of a claim, heartbeat, and release cycle
func TestPartitionLifecycleIntegration(t *testing.T) {
	srv := StartServer()

	m, err := NewMarshaler("cl", "gr", []string{srv.Addr()})
	if err != nil {
		t.Errorf("New failed: %s", err)
	}
	defer m.Terminate()

	// Claim partition (this is synchronous, will only return when)
	// it has succeeded
	resp := m.ClaimPartition("test1", 0)
	if !resp {
		t.Error("Failed to claim partition")
	}

	// Ensure we have claimed it
	cl := m.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat <= 0 || cl.ClientID != "cl" || cl.GroupID != "gr" {
		t.Error("PartitionClaim values unexpected")
	}
	if cl.LastOffset != 0 {
		t.Error("LastOffset is not 0")
	}

	// Now heartbeat on it to update the last offset
	err = m.Heartbeat("test1", 0, 10)
	if err != nil {
		t.Error("Failed to Heartbeat for partition")
	}

	// Now we have to wait for the rationalizer to update, so let's pause
	time.Sleep(500 * time.Millisecond)

	// Get the claim again, validate it's updated
	cl = m.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat <= 0 || cl.ClientID != "cl" || cl.GroupID != "gr" {
		t.Error("PartitionClaim values unexpected")
	}
	if cl.LastOffset != 10 {
		t.Error("LastOffset is not 10")
	}

	// Release
	err = m.ReleasePartition("test1", 0, 20)
	if err != nil {
		t.Error("Failed to Release for partition")
	}

	// Now we have to wait for the rationalizer to update, so let's pause
	time.Sleep(500 * time.Millisecond)

	// Get the claim again, validate it's empty
	cl = m.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat > 0 || cl.ClientID != "" || cl.GroupID != "" {
		t.Error("PartitionClaim values unexpected %s", cl)
	}
	if cl.LastOffset != 0 {
		t.Error("LastOffset is not 20")
	}

	// Get the last known claim data
	cl = m.GetLastPartitionClaim("test1", 0)
	if cl.LastHeartbeat > 0 || cl.ClientID != "cl" || cl.GroupID != "gr" {
		t.Error("PartitionClaim values unexpected %s", cl)
	}
	if cl.LastOffset != 20 {
		t.Error("LastOffset is not 20")
	}
}
