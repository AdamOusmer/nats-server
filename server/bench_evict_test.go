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
	"time"
)

func TestMemStoreEvictHeadBatch(t *testing.T) {
	defer func(old int) { benchEvictBatch = old }(benchEvictBatch)
	benchEvictBatch = 64

	subj, msg := "foo", make([]byte, 512)
	size := memStoreMsgSize(subj, nil, msg)
	const limit = 100

	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(size * limit), Subjects: []string{"foo", "bar"}})
	require_NoError(t, err)
	defer ms.Stop()

	var cbMsgs, cbBytes, removals int64
	var lastSeq uint64
	var lastSubject string
	ms.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {
		cbMsgs += md
		cbBytes += bd
		if md < 0 {
			removals++
			lastSeq = seq
			lastSubject = subj
		}
	})

	for i := 0; i < limit; i++ {
		_, _, err := ms.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}
	// One interior delete inside the run that gets evicted.
	_, err = ms.RemoveMsg(5)
	require_NoError(t, err)
	removals = 0

	_, _, err = ms.StoreMsg(subj, nil, msg, 0)
	require_NoError(t, err)

	state := ms.State()
	// 100 stored, one removed, one added: the 101st crosses the limit only
	// after the delete is accounted, so nothing should be evicted yet.
	require_Equal(t, state.Msgs, limit)
	require_Equal(t, removals, 0)

	_, _, err = ms.StoreMsg(subj, nil, msg, 0)
	require_NoError(t, err)
	state = ms.State()
	// Over by one: evict only enough to reach the byte limit.
	require_Equal(t, state.Msgs, limit)
	require_Equal(t, state.Bytes, size*limit)
	require_Equal(t, removals, 1)
	require_Equal(t, lastSeq, 1)
	require_Equal(t, lastSubject, subj)
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.NumDeleted, 1)
	require_Equal(t, uint64(cbMsgs), state.Msgs)
	require_Equal(t, uint64(cbBytes), state.Bytes)
	require_Equal(t, ms.SubjectsTotals(subj)[subj], state.Msgs)

	// Each append keeps the store exactly full.
	for i := 0; i < 8; i++ {
		_, _, err = ms.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}
	require_Equal(t, removals, 9)
	require_Equal(t, ms.State().Msgs, limit)
	require_Equal(t, ms.State().FirstSeq, 11)
	require_Equal(t, ms.State().NumDeleted, 0)

	// Lowering the limit removes several messages in a single callback.
	removals = 0
	require_NoError(t, ms.UpdateConfig(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(size * 90)}))
	state = ms.State()
	require_Equal(t, removals, 1)
	require_Equal(t, lastSeq, 0)
	require_Equal(t, lastSubject, memStoreEvictionSubject)
	require_Equal(t, state.Msgs, 90)
	require_Equal(t, state.Bytes, size*90)
	require_Equal(t, uint64(cbMsgs), state.Msgs)
	require_Equal(t, uint64(cbBytes), state.Bytes)

	// Small stores also retain every message that fits.
	small, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(3 * size)})
	require_NoError(t, err)
	defer small.Stop()
	for i := 0; i < 4; i++ {
		_, _, err = small.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}
	state = small.State()
	require_Equal(t, state.Msgs, 3)
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 4)
}

func TestMemStoreEvictHeadBatchLimits(t *testing.T) {
	defer func(old int) { benchEvictBatch = old }(benchEvictBatch)
	benchEvictBatch = 64

	msg := make([]byte, 64)
	size := memStoreMsgSize("a", nil, msg)
	for _, features := range []string{"plain", "ttl", "scheduling"} {
		for _, test := range []struct {
			name       string
			cfg        StreamConfig
			update     bool
			raw        bool
			writeSizes uint64
			removed    int64
			singles    int64
			first      uint64
			msgs       uint64
			bytes      uint64
		}{
			{name: "bytes", cfg: StreamConfig{MaxBytes: int64(20 * size)}, writeSizes: 8, removed: 8, first: 9, msgs: 13, bytes: 20 * size},
			{name: "raw-bytes", cfg: StreamConfig{MaxBytes: int64(20 * size)}, raw: true, writeSizes: 8, removed: 8, first: 9, msgs: 13, bytes: 20 * size},
			{name: "msgs", cfg: StreamConfig{MaxMsgs: 20}, writeSizes: 1, removed: 1, singles: 1, first: 2, msgs: 20, bytes: 20 * size},
			{name: "subject", cfg: StreamConfig{MaxMsgsPer: 10}, writeSizes: 1, removed: 1, singles: 1, first: 2, msgs: 20, bytes: 20 * size},
			{name: "combined-write", cfg: StreamConfig{MaxMsgs: 20, MaxBytes: int64(20 * size), MaxMsgsPer: 10}, writeSizes: 8, removed: 8, singles: 1, first: 9, msgs: 13, bytes: 20 * size},
			{name: "lower-msgs", cfg: StreamConfig{MaxMsgs: 15}, update: true, removed: 5, first: 6, msgs: 15, bytes: 15 * size},
			{name: "lower-subject", cfg: StreamConfig{MaxMsgsPer: 4}, update: true, removed: 12, singles: 12, first: 13, msgs: 8, bytes: 8 * size},
			{name: "lower-combined", cfg: StreamConfig{MaxMsgs: 15, MaxBytes: int64(10 * size), MaxMsgsPer: 4}, update: true, removed: 12, singles: 2, first: 13, msgs: 8, bytes: 8 * size},
		} {
			t.Run(features+"/"+test.name, func(t *testing.T) {
				cfg := test.cfg
				cfg.Storage = MemoryStorage
				cfg.AllowMsgTTL = features == "ttl"
				cfg.AllowMsgSchedules = features == "scheduling"
				initial := cfg
				if test.update {
					initial.MaxMsgs, initial.MaxBytes, initial.MaxMsgsPer = 0, 0, 0
				}
				ms, err := newMemStore(&initial)
				require_NoError(t, err)
				defer ms.Stop()
				var ttl int64
				if cfg.AllowMsgTTL {
					ttl = 3600
				}
				for i := 0; i < 20; i++ {
					subj := "a"
					if i%2 == 1 {
						subj = "b"
					}
					_, _, err := ms.StoreMsg(subj, nil, msg, ttl)
					require_NoError(t, err)
				}

				// Apply all limits together on a write, or lower them together in
				// UpdateConfig. Only multiple head removals use a combined update.
				var singles, markers, removed, removedBytes int64
				ms.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {
					// The callback must run after releasing the store lock.
					ms.State()
					if md < 0 {
						removed -= md
						removedBytes -= bd
						if subj == memStoreEvictionSubject {
							markers++
							require_True(t, md < -1)
							require_Equal(t, seq, 0)
						} else {
							singles++
							require_Equal(t, md, -1)
							require_True(t, seq > 0)
							wantSubject := "a"
							if seq%2 == 0 {
								wantSubject = "b"
							}
							require_Equal(t, subj, wantSubject)
						}
					}
				})
				last := uint64(20)
				if test.writeSizes == 0 {
					require_NoError(t, ms.UpdateConfig(&cfg))
				} else {
					payload := make([]byte, test.writeSizes*size-memStoreMsgSize("a", nil, nil))
					last++
					if test.raw {
						require_NoError(t, ms.StoreRawMsg("a", nil, payload, last, time.Now().UnixNano(), ttl, false))
					} else {
						seq, _, err := ms.StoreMsg("a", nil, payload, ttl)
						require_NoError(t, err)
						require_Equal(t, seq, last)
					}
				}
				require_Equal(t, singles, test.singles)
				var wantMarkers int64
				if test.removed > test.singles {
					wantMarkers = 1
				}
				require_Equal(t, markers, wantMarkers)
				require_Equal(t, removed, test.removed)
				require_Equal(t, removedBytes, test.removed*int64(size))
				state := ms.State()
				require_Equal(t, state.FirstSeq, test.first)
				require_Equal(t, state.LastSeq, last)
				require_Equal(t, state.Msgs, test.msgs)
				require_Equal(t, state.Bytes, test.bytes)
				require_Equal(t, state.NumDeleted, 0)
				for seq := uint64(1); seq <= last; seq++ {
					_, err := ms.LoadMsg(seq, nil)
					if seq < test.first {
						require_Error(t, err, ErrStoreMsgNotFound)
					} else {
						require_NoError(t, err)
					}
				}
			})
		}
	}
}
