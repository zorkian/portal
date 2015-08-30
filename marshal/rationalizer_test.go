package marshal

import (
	"testing"
)

func NewWorld() *worldState {
	return &worldState{
		quit:     new(int32),
		clientId: "cl",
		groupId:  "gr",
		topics:   make(map[string]*topicState),
	}
}

func Heartbeat(ts int, cl, gr, t string, id, lo int) *msgHeartbeat {
	return &msgHeartbeat{
		msgBase: msgBase{
			Time:     ts,
			ClientId: cl,
			GroupId:  gr,
			Topic:    t,
			PartId:   id,
		},
		LastOffset: lo,
	}
}

func TestIsClaimed(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- Heartbeat(1, "cl", "gr", "test1", 0, 0)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at ts=0")
	}

	// Should still be claimed immediately after the interval
	ws.ts = HEARTBEAT_INTERVAL + 2
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at the heartbeat boundary")
	}

	// And still claimed right at the last second of the cutoff
	ws.ts = HEARTBEAT_INTERVAL * 2
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at double the heartbeat interval")
	}

	// Should NOT be claimed >2x the heartbeat interval
	ws.ts = HEARTBEAT_INTERVAL*2 + 1
	if ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be unclaimed at double the heartbeat interval")
	}

	close(out)
}
