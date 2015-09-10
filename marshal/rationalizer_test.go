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

func NewWorld() *State {
	return &State{
		quit:     new(int32),
		clientID: "cl",
		groupID:  "gr",
		groups:   make(map[string]map[string]*topicState),
	}
}

func heartbeat(ts int, cl, gr, t string, id, lo int) *msgHeartbeat {
	return &msgHeartbeat{
		msgBase: msgBase{
			Time:     ts,
			ClientID: cl,
			GroupID:  gr,
			Topic:    t,
			PartID:   id,
		},
		LastOffset: lo,
	}
}

func claimingPartition(ts int, cl, gr, t string, id int) *msgClaimingPartition {
	return &msgClaimingPartition{
		msgBase: msgBase{
			Time:     ts,
			ClientID: cl,
			GroupID:  gr,
			Topic:    t,
			PartID:   id,
		},
	}
}

func releasingPartition(ts int, cl, gr, t string, id, lo int) *msgReleasingPartition {
	return &msgReleasingPartition{
		msgBase: msgBase{
			Time:     ts,
			ClientID: cl,
			GroupID:  gr,
			Topic:    t,
			PartID:   id,
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
	out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at ts=0")
	}

	// Should still be claimed immediately after the interval
	ws.ts = HeartbeatInterval + 2
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at the heartbeat boundary")
	}

	// And still claimed right at the last second of the cutoff
	ws.ts = HeartbeatInterval * 2
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at double the heartbeat interval")
	}

	// Should NOT be claimed >2x the heartbeat interval
	ws.ts = HeartbeatInterval*2 + 1
	if ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be unclaimed at double the heartbeat interval")
	}
}

func TestClaimNotMutable(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	cl := ws.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat == 0 {
		t.Error("Expected a claim, didn't get one")
	}

	// Modify structure, then refetch and make sure it hasn't been mutated
	cl.ClientID = "invalid"
	cl2 := ws.GetPartitionClaim("test1", 0)
	if cl2.LastHeartbeat == 0 {
		t.Error("Expected a claim, didn't get one")
	}
	if cl2.ClientID != "cl" {
		t.Error("Claim was mutated!")
	}
}

func TestClaimPartition(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// Build our return channel and insert it (simulating what the marshal does for
	// actually trying to claim)
	ret := make(chan bool, 1)
	topic := ws.getTopicState("test1", 0)
	topic.lock.Lock()
	topic.partitions[0].pendingClaims = append(topic.partitions[0].pendingClaims, ret)
	topic.lock.Unlock()

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	ws.ts = 30
	out <- claimingPartition(1, "cl", "gr", "test1", 0)

	select {
	case resp := <-ret:
		if !resp {
			t.Error("Failed to claim partition")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out claiming partition")
	}
}

func TestReclaimPartition(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// Build our return channel and insert it (simulating what the marshal does for
	// actually trying to claim)
	ret := make(chan bool, 1)
	topic := ws.getTopicState("test1", 0)
	topic.lock.Lock()
	topic.partitions[0].pendingClaims = append(topic.partitions[0].pendingClaims, ret)
	topic.lock.Unlock()

	// This log is us having the partition (HB) + a CP from someone else + a CP from us,
	// this should only fire a single 'true' into the out channel
	ws.ts = 30
	out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	out <- claimingPartition(2, "clother", "gr", "test1", 0)
	out <- claimingPartition(3, "cl", "gr", "test1", 0)

	select {
	case resp := <-ret:
		if !resp {
			t.Error("Failed to claim partition")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out claiming partition")
	}
}

func TestReleaseClaim(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at ts=0")
	}

	// Someone else attempts to release the claim, this shouldn't work
	out <- releasingPartition(20, "cl-bad", "gr", "test1", 0, 5)
	time.Sleep(5 * time.Millisecond)

	// Must be unclaimed, invalid release
	ws.ts = 25
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test:0 to be claimed at ts=25")
	}

	// Now they release it at position 10
	out <- releasingPartition(30, "cl", "gr", "test1", 0, 10)
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
	out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	ws.ts = 1
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test1:0 to be claimed at ts=0")
	}

	// Now they hand this off to someone else who picks up the heartbeat
	out <- heartbeat(10, "cl2", "gr", "test1", 0, 10)
	time.Sleep(5 * time.Millisecond)

	// Must be claimed, and claimed by cl2
	ws.ts = 25
	if !ws.IsClaimed("test1", 0) {
		t.Error("Expected test:0 to be claimed at ts=25")
	}
	if ws.GetPartitionClaim("test1", 0).ClientID != "cl2" {
		t.Error("Expected claim by cl2, but wasn't")
	}

	// Now we change the group ID of our world state (which client's can't do) and validate
	// that these partitions are NOT claimed
	ws.ts = 25
	ws.groupID = "gr2"
	if ws.IsClaimed("test1", 0) {
		t.Error("Expected test:0 to be unclaimed at ts=25")
	}
	if ws.GetPartitionClaim("test1", 0).ClientID != "" {
		t.Error("Expected unclaimed, but was")
	}
}

func TestPartitionExtend(t *testing.T) {
	ws := NewWorld()
	out := make(chan message)
	defer close(out)
	go ws.rationalize(0, out)

	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// Ensure len is 1
	ws.lock.RLock()
	ws.groups["gr"]["test1"].lock.RLock()
	if len(ws.groups["gr"]["test1"].partitions) != 1 {
		t.Error("Expected only 1 partition")
	}
	ws.groups["gr"]["test1"].lock.RUnlock()
	ws.lock.RUnlock()

	// Extend by 4
	out <- heartbeat(2, "cl2", "gr", "test1", 4, 0)
	time.Sleep(5 * time.Millisecond)

	// Ensure len is 5
	ws.lock.RLock()
	defer ws.lock.RUnlock()
	ws.groups["gr"]["test1"].lock.RLock()
	defer ws.groups["gr"]["test1"].lock.RUnlock()

	if len(ws.groups["gr"]["test1"].partitions) != 5 {
		t.Error("Expected only 5 partitions")
	}

	// Ensure 0 and 4 are claimed by us
	p1 := ws.groups["gr"]["test1"].partitions[0]
	p2 := ws.groups["gr"]["test1"].partitions[4]
	if p1.ClientID != "cl" || p1.GroupID != "gr" || p1.LastHeartbeat != 1 ||
		p2.ClientID != "cl2" || p2.GroupID != "gr" || p2.LastHeartbeat != 2 {
		t.Error("Partition contents unexpected")
	}
}
