package marshal

import (
	"testing"
)

func TestMessageEncode(t *testing.T) {
	base := msgBase{
		ClientId: "cl",
		GroupId:  "gr",
		Topic:    "t",
		PartId:   3,
	}

	bstr := base.Encode()
	if bstr != "cl/gr/t/3" {
		t.Error("Base message string wrong:", bstr)
	}

	hb := msgHeartbeat{
		msgBase:    base,
		LastOffset: 5,
	}
	hbstr := hb.Encode()
	if hbstr != "Heartbeat/cl/gr/t/3/5" {
		t.Error("Heartbeat message string wrong:", hbstr)
	}

	cp := msgClaimingPartition{
		msgBase: base,
	}
	cpstr := cp.Encode()
	if cpstr != "ClaimingPartition/cl/gr/t/3" {
		t.Error("ClaimingPartition message string wrong:", cpstr)
	}

	rp := msgReleasingPartition{
		msgBase:    base,
		LastOffset: 7,
	}
	rpstr := rp.Encode()
	if rpstr != "ReleasingPartition/cl/gr/t/3/7" {
		t.Error("ReleasingPartition message string wrong:", rpstr)
	}

	cm := msgClaimingMessages{
		msgBase:            base,
		ProposedLastOffset: 9,
	}
	cmstr := cm.Encode()
	if cmstr != "ClaimingMessages/cl/gr/t/3/9" {
		t.Error("ClaimingMessages message string wrong:", cmstr)
	}
}

func TestMessageDecode(t *testing.T) {
	msg, err := Decode([]byte("banana"))
	if msg != nil || err == nil {
		t.Error("Expected error, got msg", msg)
	}

	msg, err = Decode([]byte("Heartbeat/cl/gr/t/1/2"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mhb, ok := msg.(*msgHeartbeat)
	if !ok || msg.Type() != MsgHeartbeat || mhb.ClientId != "cl" || mhb.GroupId != "gr" ||
		mhb.Topic != "t" || mhb.PartId != 1 || mhb.LastOffset != 2 {
		t.Error("Heartbeat message contents invalid")
	}

	msg, err = Decode([]byte("ClaimingPartition/cl/gr/t/1"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mcp, ok := msg.(*msgClaimingPartition)
	if !ok || msg.Type() != MsgClaimingPartition || mcp.ClientId != "cl" ||
		mcp.GroupId != "gr" || mcp.Topic != "t" || mcp.PartId != 1 {
		t.Error("ClaimingPartition message contents invalid")
	}

	msg, err = Decode([]byte("ReleasingPartition/cl/gr/t/1"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mrp, ok := msg.(*msgReleasingPartition)
	if !ok || msg.Type() != MsgReleasingPartition || mrp.ClientId != "cl" ||
		mrp.GroupId != "gr" || mrp.Topic != "t" || mrp.PartId != 1 {
		t.Error("ReleasingPartition message contents invalid")
	}

	msg, err = Decode([]byte("ClaimingMessages/cl/gr/t/1/2"))
	if msg == nil || err != nil {
		t.Error("Expected msg, got error", err)
	}
	mcm, ok := msg.(*msgClaimingMessages)
	if !ok || msg.Type() != MsgClaimingMessages || mcm.ClientId != "cl" || mcm.GroupId != "gr" ||
		mcm.Topic != "t" || mcm.PartId != 1 || mcm.ProposedLastOffset != 2 {
		t.Error("ClaimingMessages message contents invalid")
	}
}
