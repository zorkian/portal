package marshal

import (
	"testing"
)

func TestMessageEncode(t *testing.T) {
	base := msgBase{
		Time:     2,
		ClientID: "cl",
		GroupID:  "gr",
		Topic:    "t",
		PartID:   3,
	}

	bstr := base.Encode()
	if bstr != "2/cl/gr/t/3" {
		t.Error("Base message string wrong:", bstr)
	}

	hb := msgHeartbeat{
		msgBase:    base,
		LastOffset: 5,
	}
	hbstr := hb.Encode()
	if hbstr != "Heartbeat/2/cl/gr/t/3/5" {
		t.Error("Heartbeat message string wrong:", hbstr)
	}

	cp := msgClaimingPartition{
		msgBase: base,
	}
	cpstr := cp.Encode()
	if cpstr != "ClaimingPartition/2/cl/gr/t/3" {
		t.Error("ClaimingPartition message string wrong:", cpstr)
	}

	rp := msgReleasingPartition{
		msgBase:    base,
		LastOffset: 7,
	}
	rpstr := rp.Encode()
	if rpstr != "ReleasingPartition/2/cl/gr/t/3/7" {
		t.Error("ReleasingPartition message string wrong:", rpstr)
	}

	cm := msgClaimingMessages{
		msgBase:            base,
		ProposedLastOffset: 9,
	}
	cmstr := cm.Encode()
	if cmstr != "ClaimingMessages/2/cl/gr/t/3/9" {
		t.Error("ClaimingMessages message string wrong:", cmstr)
	}
}

func TestMessageDecode(t *testing.T) {
	msg, err := decode([]byte("banana"))
	if msg != nil || err == nil {
		t.Error("Expected error, got msg", msg)
	}

	msg, err = decode([]byte("Heartbeat/2/cl/gr/t/1/2"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mhb, ok := msg.(*msgHeartbeat)
	if !ok || msg.Type() != msgTypeHeartbeat || mhb.ClientID != "cl" || mhb.GroupID != "gr" ||
		mhb.Topic != "t" || mhb.PartID != 1 || mhb.LastOffset != 2 || mhb.Time != 2 {
		t.Error("Heartbeat message contents invalid")
	}

	msg, err = decode([]byte("ClaimingPartition/2/cl/gr/t/1"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mcp, ok := msg.(*msgClaimingPartition)
	if !ok || msg.Type() != msgTypeClaimingPartition || mcp.ClientID != "cl" ||
		mcp.GroupID != "gr" || mcp.Topic != "t" || mcp.PartID != 1 || mcp.Time != 2 {
		t.Error("ClaimingPartition message contents invalid")
	}

	msg, err = decode([]byte("ReleasingPartition/2/cl/gr/t/1/9"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mrp, ok := msg.(*msgReleasingPartition)
	if !ok || msg.Type() != msgTypeReleasingPartition || mrp.ClientID != "cl" ||
		mrp.GroupID != "gr" || mrp.Topic != "t" || mrp.PartID != 1 || mrp.Time != 2 ||
		mrp.LastOffset != 9 {
		t.Error("ReleasingPartition message contents invalid")
	}

	msg, err = decode([]byte("ClaimingMessages/2/cl/gr/t/1/2"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mcm, ok := msg.(*msgClaimingMessages)
	if !ok || msg.Type() != msgTypeClaimingMessages || mcm.ClientID != "cl" || mcm.GroupID != "gr" ||
		mcm.Topic != "t" || mcm.PartID != 1 || mcm.ProposedLastOffset != 2 || mcm.Time != 2 {
		t.Error("ClaimingMessages message contents invalid")
	}
}
