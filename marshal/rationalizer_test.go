package marshal

import (
	"testing"

	"github.com/optiopay/kafka/proto"
)

func TestParseLog(t *testing.T) {
	srv := StartServer()
	base := msgBase{
		ClientId: "cl",
		GroupId:  "gr",
		Topic:    "t",
		PartId:   0,
	}
	srv.AddMessages(MARSHAL_TOPIC, 0,
		&proto.Message{Value: []byte(
			(&msgHeartbeat{msgBase: base, LastOffset: 10}).Encode())},
		&proto.Message{Value: []byte(
			(&msgHeartbeat{msgBase: base, LastOffset: 10}).Encode())},
		&proto.Message{Value: []byte(
			(&msgHeartbeat{msgBase: base, LastOffset: 10}).Encode())},
	)

	m, err := NewMarshaler("cl", "gr", []string{srv.Addr()})
	if err != nil {
		t.Errorf("New failed: %s", err)
	}
	defer m.Terminate()

}
