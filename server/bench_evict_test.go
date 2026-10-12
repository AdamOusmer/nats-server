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

import "testing"

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
	ms.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {
		cbMsgs += md
		cbBytes += bd
		if md < 0 {
			removals++
			lastSeq = seq
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
	require_Equal(t, lastSeq, 0)
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
