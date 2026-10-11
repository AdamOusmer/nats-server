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
	"testing"

	"github.com/nats-io/nats-server/v2/server/thw"
)

func TestMemStoreSlabIsolation(t *testing.T) {
	defer func(old bool) { benchStoreSlab = old }(benchStoreSlab)
	benchStoreSlab = true

	for _, ms := range []*memStore{
		{ttls: thw.NewHashWheel()},
		{cfg: StreamConfig{Retention: InterestPolicy}},
		{cfg: StreamConfig{MaxMsgsPer: 1, Subjects: []string{"kv.>"}}},
	} {
		if sm := ms.newStoreMsg("x.y", 64, 1, 1); cap(sm.buf) != 64 || ms.slabBuf != nil || ms.slabMsgs != nil {
			t.Fatalf("store %+v took a slab", ms.cfg)
		}
	}

	ms := &memStore{cfg: StreamConfig{MaxMsgsPer: 400_000, Subjects: []string{"lane"}}}
	var prev *StoreMsg
	for i := range 5000 {
		n := 1 + i%700
		sm := ms.newStoreMsg("lane", n, uint64(i), int64(i))
		if cap(sm.buf) != n || len(sm.buf) != 0 {
			t.Fatalf("i=%d cap=%d len=%d, want cap %d len 0", i, cap(sm.buf), len(sm.buf), n)
		}
		sm.buf = append(sm.buf, make([]byte, n)...)
		for j := range sm.buf {
			sm.buf[j] = byte(i)
		}
		if prev != nil {
			for j := range prev.buf {
				if prev.buf[j] != byte(i-1) {
					t.Fatalf("i=%d overwrote the previous message's byte %d", i, j)
				}
			}
			if prev.seq != uint64(i-1) {
				t.Fatalf("i=%d reused the previous StoreMsg: seq %d", i, prev.seq)
			}
		}
		prev = sm
	}
	if ms.slabBuf == nil {
		t.Fatal("eligible store did not use a slab")
	}
}
