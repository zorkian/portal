package marshal

import (
	"testing"
)

func TestMessageEncode(t *testing.T) {
	base := msgBase{
		clientId: "cl",
		groupId:  "gr",
		topic:    "t",
		partId:   3,
	}

	bstr := base.Encode()
	if bstr != "cl/gr/t/3" {
		t.Error("Base message string wrong:", bstr)
	}

	hb := msgHeartbeat{
		msgBase:    base,
		lastOffset: 5,
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
		lastOffset: 7,
	}
	rpstr := rp.Encode()
	if rpstr != "ReleasingPartition/cl/gr/t/3/7" {
		t.Error("ReleasingPartition message string wrong:", rpstr)
	}

	cm := msgClaimingMessages{
		msgBase:            base,
		proposedLastOffset: 9,
	}
	cmstr := cm.Encode()
	if cmstr != "ClaimingMessages/cl/gr/t/3/9" {
		t.Error("ClaimingMessages message string wrong:", cmstr)
	}
}
