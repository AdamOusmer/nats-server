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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamPerSubjectLimitPendingTermination(t *testing.T) {
	defer func(old int) { benchEvictBatch = old }(benchEvictBatch)
	benchEvictBatch = 64
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST", Subjects: []string{"a", "b"}, Storage: nats.MemoryStorage, MaxMsgsPerSubject: 1,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable: "C", FilterSubject: "a", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Hour,
	})
	require_NoError(t, err)
	terms, err := nc.SubscribeSync(JSAdvisoryConsumerMsgTerminatedPre + ".TEST.C")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())
	_, err = js.Publish("b", nil)
	require_NoError(t, err)
	_, err = js.Publish("a", nil)
	require_NoError(t, err)
	sub, err := js.PullSubscribe("a", "C", nats.Bind("TEST", "C"))
	require_NoError(t, err)
	require_Len(t, len(fetchMsgs(t, sub, 1, 5*time.Second)), 1)
	require_NoError(t, sub.Unsubscribe())
	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.NumAckPending, 1)
	_, err = js.Publish("a", nil)
	require_NoError(t, err)
	m, err := terms.NextMsg(5 * time.Second)
	require_NoError(t, err)
	var advisory JSConsumerDeliveryTerminatedAdvisory
	require_NoError(t, json.Unmarshal(m.Data, &advisory))
	require_Equal(t, advisory.StreamSeq, 2)
	require_Equal(t, advisory.Reason, ackTermUnackedLimitsReason)
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "C")
		if err != nil {
			return err
		}
		if ci.NumAckPending != 0 {
			return fmt.Errorf("expected no ack pending, got %d", ci.NumAckPending)
		}
		return nil
	})
	_, err = terms.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestMemStoreEvictHeadBatchSteadySingleRemoval(t *testing.T) {
	defer func(old int) { benchEvictBatch = old }(benchEvictBatch)
	benchEvictBatch = 64
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw=%t", raw), func(t *testing.T) {
			ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxMsgs: 10})
			require_NoError(t, err)
			defer ms.Stop()
			for range 10 {
				_, _, err := ms.StoreMsg("a", nil, nil, 0)
				require_NoError(t, err)
			}
			var markers, singles int
			ms.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {
				if md >= 0 {
					return
				}
				if subj == memStoreEvictionSubject {
					markers++
					return
				}
				singles++
				require_Equal(t, md, -1)
				require_Equal(t, bd, -int64(memStoreMsgSize("a", nil, nil)))
				require_Equal(t, seq, uint64(singles))
				require_Equal(t, subj, "a")
			})
			for i := 0; i < 100; i++ {
				if raw {
					err = ms.StoreRawMsg("a", nil, nil, uint64(11+i), time.Now().UnixNano(), 0, false)
				} else {
					_, _, err = ms.StoreMsg("a", nil, nil, 0)
				}
				require_NoError(t, err)
			}
			require_Equal(t, markers, 0)
			require_Equal(t, singles, 100)
			require_Equal(t, ms.State().Msgs, 10)
		})
	}
}

func BenchmarkR3EvictSteady(b *testing.B) {
	for _, filter := range []string{"s.*", "s.7"} {
		b.Run(filter, func(b *testing.B) {
			const limit = 1000
			ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxMsgs: limit})
			if err != nil {
				b.Fatal(err)
			}
			defer ms.Stop()
			subjects := make([]string, limit)
			payload := make([]byte, 100)
			for i := range subjects {
				subjects[i] = fmt.Sprintf("s.%d", i)
				if _, _, err := ms.StoreMsg(subjects[i], nil, payload, 0); err != nil {
					b.Fatal(err)
				}
			}
			mset := &stream{store: ms}
			o := consumerWithFilterSubjects([]string{filter})
			o.mset, o.sseq = mset, 1
			o.leader.Store(true)
			o.recalculateStreamPending()
			mset.cList = []*consumer{o}
			ms.RegisterStorageUpdates(mset.storeUpdates)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := ms.StoreMsg(subjects[i%limit], nil, payload, 0); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/1000, "µs/op")
		})
	}
}
