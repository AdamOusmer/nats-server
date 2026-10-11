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

// With NATS_BENCH_STORE_SLAB=1 the memory store carves its two per-message
// allocations, the StoreMsg and its buffer, out of shared slabs. A slab stays
// alive while any of its messages is referenced, so slabs are only used by
// stores that remove oldest-first and retire slabs in the order they filled.
// A larger message gets its own exact allocation.
const (
	storeSlabMsgs  = 256   // StoreMsg headers per slab.
	storeSlabBytes = 65536 // Buffer bytes per slab.
)

// slabEligible reports whether messages leave this store oldest-first. TTLs,
// schedules, acknowledgement-driven retention and per-subject limits across
// several subjects remove from the middle, where one survivor would pin a
// whole slab.
// Lock should be held.
func (ms *memStore) slabEligible() bool {
	if !benchStoreSlab || ms.ttls != nil || ms.scheduling != nil || ms.cfg.Retention != LimitsPolicy {
		return false
	}
	return ms.cfg.MaxMsgsPer <= 0 || len(ms.cfg.Subjects) == 1 && subjectIsLiteral(ms.cfg.Subjects[0])
}

// newStoreMsg returns a StoreMsg whose buf has length 0 and capacity n.
// Lock should be held.
func (ms *memStore) newStoreMsg(subj string, n int, seq uint64, ts int64) *StoreMsg {
	if !ms.slabEligible() {
		return &StoreMsg{subj, nil, nil, make([]byte, 0, n), seq, ts}
	}
	if len(ms.slabMsgs) == 0 {
		ms.slabMsgs = make([]StoreMsg, storeSlabMsgs)
	}
	sm := &ms.slabMsgs[0]
	ms.slabMsgs = ms.slabMsgs[1:]
	*sm = StoreMsg{subj: subj, buf: ms.slabBytes(n), seq: seq, ts: ts}
	return sm
}

// slabBytes returns an empty slice with capacity exactly n, so an append can
// never reach into the next message's bytes.
// Lock should be held.
func (ms *memStore) slabBytes(n int) []byte {
	if n > storeSlabBytes/4 {
		return make([]byte, 0, n)
	}
	if len(ms.slabBuf) < n {
		ms.slabBuf = make([]byte, storeSlabBytes)
	}
	buf := ms.slabBuf[:0:n]
	ms.slabBuf = ms.slabBuf[n:]
	return buf
}
