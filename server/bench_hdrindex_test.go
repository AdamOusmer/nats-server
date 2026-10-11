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
	"bytes"
	"fmt"
	"testing"
)

func TestHdrIndexMatchesSliceHeader(t *testing.T) {
	defer func(old bool) { benchHdrIndex = old }(benchHdrIndex)
	benchHdrIndex = true

	many := genHeader(nil, JSMsgId, "id")
	for i := 0; i < 20; i++ {
		many = genHeader(many, fmt.Sprintf("X-%d", i), "v")
	}
	hdrs := [][]byte{
		nil,
		[]byte("NATS/1.0\r\n\r\n"),
		genHeader(nil, JSMsgId, "abc"),
		genHeader(genHeader(nil, JSBatchId, "b1"), JSBatchSeq, "7"),
		[]byte("NATS/1.0\r\nNats-Msg-Id:   spaced\r\nNats-Msg-Id: second\r\nNats-Schedule:\r\n\r\n"),
		[]byte("NATS/1.0\r\nX-Nats-Msg-Id: no\r\nNats-Msg-Id : no\r\nNats-Rollup: sub\r\n\r\n"),
		[]byte("NATS/1.0\r\nNats-TTL: 5s"),
		many,
	}
	keys := []string{JSMsgId, JSBatchId, JSBatchSeq, JSSchedulePattern, JSMsgRollup, JSMessageTTL, "X-3", "X-19", "Missing"}
	for _, hdr := range hdrs {
		hx := indexHeader(hdr)
		for _, key := range keys {
			want, got := sliceHeader(key, hdr), hx.slice(key, hdr)
			if (want == nil) != (got == nil) || !bytes.Equal(want, got) || cap(got) != len(got) {
				t.Fatalf("hdr %q key %q: want %q, got %q (cap %d)", hdr, key, want, got, cap(got))
			}
		}
	}

	hdr := genHeader(nil, JSMsgId, "abc")
	hx := indexHeader(hdr)
	hdr = genHeader(hdr, JSMsgRollup, "sub")
	if got := hx.slice(JSMsgRollup, hdr); string(got) != "sub" {
		t.Fatalf("stale index served %q", got)
	}
	hx.refresh(hdr)
	if got := hx.slice(JSMsgRollup, hdr); string(got) != "sub" || !hx.ok {
		t.Fatalf("refreshed index served %q", got)
	}
}
