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

import "iter"

// msgIndex is the memory store's sequence to message index. The stock map
// pays a hash insert and delete per message and rehashes as it grows. Stored
// sequences are contiguous apart from deletions, so with the knob set a
// power-of-two ring indexed by seq&mask replaces it while the live span stays
// dense. A ring that would grow past ringMaxSparsity times its live entries,
// for example behind one old survivor of a per-subject limit, turns into a map.
type msgIndex interface {
	get(seq uint64) (*StoreMsg, bool)
	set(seq uint64, sm *StoreMsg)
	del(seq uint64)
	len() int
	all() iter.Seq[*StoreMsg]
}

const (
	ringMinSize     = 1024
	ringMaxSparsity = 4
)

func newMsgIndex() msgIndex {
	if benchRingIndex {
		return &ringMsgIndex{buf: make([]*StoreMsg, ringMinSize), mask: ringMinSize - 1}
	}
	return mapMsgIndex{}
}

type mapMsgIndex map[uint64]*StoreMsg

func (m mapMsgIndex) get(seq uint64) (*StoreMsg, bool) { sm, ok := m[seq]; return sm, ok }
func (m mapMsgIndex) set(seq uint64, sm *StoreMsg)     { m[seq] = sm }
func (m mapMsgIndex) del(seq uint64)                   { delete(m, seq) }
func (m mapMsgIndex) len() int                         { return len(m) }
func (m mapMsgIndex) all() iter.Seq[*StoreMsg] {
	return func(yield func(*StoreMsg) bool) {
		for _, sm := range m {
			if !yield(sm) {
				return
			}
		}
	}
}

type ringMsgIndex struct {
	buf  []*StoreMsg
	mask uint64
	base uint64 // Lowest live sequence, valid when n > 0.
	hi   uint64 // Highest live sequence, valid when n > 0.
	n    int
}

func (r *ringMsgIndex) inRange(seq uint64) bool {
	return r.n > 0 && seq >= r.base && seq-r.base < uint64(len(r.buf))
}

func (r *ringMsgIndex) get(seq uint64) (*StoreMsg, bool) {
	if !r.inRange(seq) {
		return nil, false
	}
	sm := r.buf[seq&r.mask]
	return sm, sm != nil
}

// tooSparse reports whether storing seq would grow the ring beyond
// ringMaxSparsity slots per live entry.
func (r *ringMsgIndex) tooSparse(seq uint64) bool {
	if r.n == 0 {
		return false
	}
	lo, hi := min(r.base, seq), max(r.hi, seq)
	span := hi - lo + 1
	if span <= uint64(len(r.buf)) {
		return false
	}
	size := uint64(len(r.buf))
	for size < span {
		size *= 2
	}
	return size > ringMaxSparsity*uint64(r.n+1)
}

func (r *ringMsgIndex) set(seq uint64, sm *StoreMsg) {
	if r.n == 0 {
		r.base, r.hi = seq, seq
	}
	r.base, r.hi = min(r.base, seq), max(r.hi, seq)
	for r.hi-r.base >= uint64(len(r.buf)) {
		r.grow()
	}
	slot := &r.buf[seq&r.mask]
	if *slot == nil {
		r.n++
	}
	*slot = sm
}

func (r *ringMsgIndex) grow() {
	r.resize(2 * len(r.buf))
}

func (r *ringMsgIndex) resize(size int) {
	nbuf := make([]*StoreMsg, size)
	nmask := uint64(len(nbuf) - 1)
	for _, sm := range r.buf {
		if sm != nil {
			nbuf[sm.seq&nmask] = sm
		}
	}
	r.buf, r.mask = nbuf, nmask
}

func (r *ringMsgIndex) del(seq uint64) {
	if !r.inRange(seq) {
		return
	}
	slot := &r.buf[seq&r.mask]
	if *slot == nil {
		return
	}
	*slot = nil
	if r.n--; r.n == 0 {
		r.base, r.hi = 0, 0
		if len(r.buf) > ringMinSize {
			r.resize(ringMinSize)
		}
		return
	}
	if seq == r.hi {
		for r.buf[r.hi&r.mask] == nil {
			r.hi--
		}
	}
	for seq == r.base && r.buf[r.base&r.mask] == nil {
		r.base++
		seq++
	}
}

func (r *ringMsgIndex) shrink() {
	if r.n == 0 {
		return
	}
	size := ringMinSize
	for uint64(size) <= r.hi-r.base {
		size *= 2
	}
	if size < len(r.buf) {
		r.resize(size)
	}
}

func (r *ringMsgIndex) len() int { return r.n }

func (r *ringMsgIndex) all() iter.Seq[*StoreMsg] {
	return func(yield func(*StoreMsg) bool) {
		for _, sm := range r.buf {
			if sm != nil && !yield(sm) {
				return
			}
		}
	}
}

func (r *ringMsgIndex) toMap() mapMsgIndex {
	m := make(mapMsgIndex, r.n)
	for _, sm := range r.buf {
		if sm != nil {
			m[sm.seq] = sm
		}
	}
	return m
}

// The accessors below keep the nil-map semantics the store relies on after
// Stop sets ms.msgs to nil: zero length, no-op delete, lookups that miss.

func (ms *memStore) idxget(seq uint64) (*StoreMsg, bool) {
	if ms.msgs == nil {
		return nil, false
	}
	return ms.msgs.get(seq)
}

func (ms *memStore) idxget1(seq uint64) *StoreMsg {
	sm, _ := ms.idxget(seq)
	return sm
}

func (ms *memStore) idxset(seq uint64, sm *StoreMsg) {
	if r, ok := ms.msgs.(*ringMsgIndex); ok && r.tooSparse(seq) {
		ms.msgs = r.toMap()
	}
	ms.msgs.set(seq, sm)
}

func (ms *memStore) idxdel(seq uint64) {
	if ms.msgs != nil {
		ms.msgs.del(seq)
	}
}

func (ms *memStore) idxlen() int {
	if ms.msgs == nil {
		return 0
	}
	return ms.msgs.len()
}

func (ms *memStore) idxall() iter.Seq[*StoreMsg] {
	if ms.msgs == nil {
		return func(func(*StoreMsg) bool) {}
	}
	return ms.msgs.all()
}
