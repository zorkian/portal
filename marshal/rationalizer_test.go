package marshal

import (
	"testing"
	"time"

	"github.com/op/go-logging"
)

func init() {
	// TODO: This changes logging for the whole suite. Is that what we want?
	logging.SetLevel(logging.ERROR, "PortalMarshal")
}

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

func ReleasingPartition(ts int, cl, gr, t string, id, lo int) *msgReleasingPartition {
	return &msgReleasingPartition{
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
	defer close(out)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- Heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

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
}

func TestReleaseClaim(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- Heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at ts=0")
	}

	// Someone else attempts to release the claim, this shouldn't work
	out <- ReleasingPartition(20, "cl-bad", "gr", "test1", 0, 5)
	time.Sleep(5 * time.Millisecond)

	// Must be unclaimed, invalid release
	ws.ts = 25
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test:0 to be claimed at ts=25")
	}

	// Now they release it at position 10
	out <- ReleasingPartition(30, "cl", "gr", "test1", 0, 10)
	time.Sleep(5 * time.Millisecond)

	// They released at 30, should be free as of 31
	ws.ts = 31
	if ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be unclaimed at ts=31")
	}
}

func TestClaimHandoff(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- Heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at ts=0")
	}

	// Now they hand this off to someone else who picks up the heartbeat
	out <- Heartbeat(10, "cl2", "gr", "test1", 0, 10)
	time.Sleep(5 * time.Millisecond)

	// Must be claimed, and claimed by cl2
	ws.ts = 25
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test:0 to be claimed at ts=25")
	}
	if ws.GetPartitionClaim("test1", 0).ClientId != "cl2" {
		t.Error("Expected claim by cl2, but wasn't")
	}
}
