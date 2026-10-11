// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"math/rand"
	"testing"
)

func TestMsgIndexRingMatchesMap(t *testing.T) {
	defer func(old bool) { benchRingIndex = old }(benchRingIndex)
	benchRingIndex = true

	ms := &memStore{msgs: newMsgIndex()}
	ref := map[uint64]*StoreMsg{}
	rng := rand.New(rand.NewSource(1))
	var next uint64 = 1
	check := func(seq uint64) {
		t.Helper()
		want, wok := ref[seq]
		got, gok := ms.idxget(seq)
		if wok != gok || want != got {
			t.Fatalf("seq %d: want %v/%v, got %v/%v", seq, want, wok, got, gok)
		}
	}
	for i := 0; i < 200_000; i++ {
		switch op := rng.Intn(10); {
		case op < 6:
			sm := &StoreMsg{seq: next}
			ms.idxset(next, sm)
			ref[next] = sm
			next++
		case op < 9 && len(ref) > 0:
			// Mostly delete from the head, sometimes from the middle.
			seq := next - uint64(len(ref))
			if rng.Intn(4) == 0 {
				seq += uint64(rng.Intn(len(ref)))
			}
			ms.idxdel(seq)
			delete(ref, seq)
		default:
			check(next - uint64(rng.Intn(int(next))))
		}
		if ms.idxlen() != len(ref) {
			t.Fatalf("len %d, want %d", ms.idxlen(), len(ref))
		}
	}
	for seq := range next + 10 {
		check(seq)
	}
	var n int
	for sm := range ms.idxall() {
		if ref[sm.seq] != sm {
			t.Fatalf("all yielded stray seq %d", sm.seq)
		}
		n++
	}
	if n != len(ref) {
		t.Fatalf("all yielded %d, want %d", n, len(ref))
	}

	// One old survivor ahead of a long run must not grow the ring without bound.
	ms = &memStore{msgs: newMsgIndex()}
	ms.idxset(1, &StoreMsg{seq: 1})
	for seq := uint64(2); seq < 1_000_000; seq++ {
		ms.idxset(seq, &StoreMsg{seq: seq})
		ms.idxdel(seq - 1)
		if seq == 2 {
			ms.idxset(1, &StoreMsg{seq: 1})
		}
	}
	if r, ok := ms.msgs.(*ringMsgIndex); ok && len(r.buf) > 4*ringMinSize {
		t.Fatalf("ring grew to %d slots for %d live entries", len(r.buf), r.n)
	}
	if sm, ok := ms.idxget(1); !ok || sm.seq != 1 {
		t.Fatal("lost the survivor")
	}
}
