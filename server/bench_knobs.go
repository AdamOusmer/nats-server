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
	"os"
	"runtime"
	"strconv"
	"time"
)

// Tuning knobs are read once from NATS_BENCH_* environment variables at
// startup. Every knob is off when its variable is unset, which keeps the stock
// behavior.
var (
	// benchTLSCollapse joins the pending outbound buffers of a TLS connection
	// into one write, so a flush becomes one TLS record instead of one per
	// buffer.
	benchTLSCollapse = benchFlag("NATS_BENCH_TLS_COLLAPSE")
	// benchProposeCoalesce delays a consumer's proposal forwarder by this
	// duration after a kick, so one append entry carries more updates.
	benchProposeCoalesce = benchDuration("NATS_BENCH_PROPOSE_COALESCE")
	// benchAckNoneEvery replicates an AckNone consumer's delivered cursor
	// every n deliveries instead of every delivery. A new consumer leader
	// can resend up to n-1 messages the old one already delivered.
	benchAckNoneEvery = benchUint("NATS_BENCH_ACKNONE_PROPOSE_EVERY")
	// benchDeliverBatch keeps the consumer lock across up to n consecutive
	// deliveries instead of releasing it after each one.
	benchDeliverBatch = int(benchUint("NATS_BENCH_DELIVER_BATCH"))
	// benchSignalCoalesce signals each consumer once per batch of stored
	// messages instead of once per message.
	benchSignalCoalesce = benchFlag("NATS_BENCH_SIGNAL_COALESCE")
	// benchHdrIndex parses a message's header block once on the ingest and
	// apply paths instead of scanning it once per header lookup.
	benchHdrIndex = benchFlag("NATS_BENCH_HDR_INDEX")
	// benchStageArena stages in-memory atomic batches in a slice instead of
	// a private memory store.
	benchStageArena = benchFlag("NATS_BENCH_STAGE_ARENA")
	// benchRingIndex indexes memory store messages in a ring by sequence
	// instead of a map.
	benchRingIndex = benchFlag("NATS_BENCH_RING_INDEX")
)

func benchFlag(key string) bool {
	return os.Getenv(key) == "1"
}

func benchDuration(key string) time.Duration {
	d, _ := time.ParseDuration(os.Getenv(key))
	return d
}

func benchUint(key string) uint64 {
	n, _ := strconv.ParseUint(os.Getenv(key), 10, 64)
	return n
}

// NATS_BENCH_LOCK_PROFILE=1 samples mutex contention (1 in 20) and blocking
// events of 50us or longer, so /debug/pprof/mutex and /block name the locks.
func init() {
	if benchFlag("NATS_BENCH_LOCK_PROFILE") {
		runtime.SetMutexProfileFraction(20)
		runtime.SetBlockProfileRate(50_000)
	}
}
