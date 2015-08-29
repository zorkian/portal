package marshal

import (
	"testing"

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
