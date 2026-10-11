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
	"unsafe"
)

// hdrIndex is a parse-once view of one message's header block. The ingest and
// apply paths probe the same block for many keys per message, each probe a
// scan of the whole block. The index is built once per message, answers keys
// it knows are absent with a bit test, and finds present keys in a short list.
// It is bound to one exact slice (data pointer and length): a lookup with any
// other slice, or with the knob off, falls back to sliceHeader. Callers
// refresh it after rewriting hdr so the index stays useful.
type hdrIndex struct {
	data    *byte
	n       int
	nk      int
	present uint64
	ok      bool
	keys    [16]hdrIndexKey
}

type hdrIndexKey struct{ ks, kl, vs, ve int32 }

var crlfBytes = []byte(_CRLF_)

func indexHeader(hdr []byte) hdrIndex {
	var hx hdrIndex
	if !benchHdrIndex || len(hdr) < 2 || len(hdr) > 1<<30 {
		return hx
	}
	hx.data, hx.n = unsafe.SliceData(hdr), len(hdr)
	i := bytes.Index(hdr, crlfBytes)
	for i >= 0 {
		start := i + 2
		if start >= len(hdr) {
			break
		}
		rest := hdr[start:]
		nl := bytes.Index(rest, crlfBytes)
		if nl < 0 {
			nl = len(rest)
		}
		line := rest[:nl]
		if colon := bytes.IndexByte(line, ':'); colon > 0 {
			if hx.nk == len(hx.keys) {
				return hdrIndex{}
			}
			vs := colon + 1
			for vs < nl && line[vs] == ' ' {
				vs++
			}
			hx.keys[hx.nk] = hdrIndexKey{int32(start), int32(colon), int32(start + vs), int32(start + nl)}
			hx.nk++
			if bit, known := knownHeaderBit(bytesToString(line[:colon])); known {
				hx.present |= 1 << bit
			}
		}
		if start+nl >= len(hdr) {
			break
		}
		i = start + nl
	}
	hx.ok = true
	return hx
}

// refresh re-parses only when hdr is a different slice from the one indexed.
func (hx *hdrIndex) refresh(hdr []byte) {
	if hx.ok && hx.n == len(hdr) && hx.data == unsafe.SliceData(hdr) {
		return
	}
	*hx = indexHeader(hdr)
}

// slice mirrors sliceHeader: the value bytes of key inside hdr, or nil.
func (hx *hdrIndex) slice(key string, hdr []byte) []byte {
	if hx == nil || !hx.ok || hx.n != len(hdr) || hx.data != unsafe.SliceData(hdr) {
		return sliceHeader(key, hdr)
	}
	if bit, known := knownHeaderBit(key); known && hx.present&(1<<bit) == 0 {
		return nil
	}
	for i := 0; i < hx.nk; i++ {
		k := &hx.keys[i]
		if int(k.kl) == len(key) && bytesToString(hdr[k.ks:k.ks+k.kl]) == key {
			return hdr[k.vs:k.ve:k.ve]
		}
	}
	return nil
}

// knownHeaderBit maps the headers the JetStream paths probe to a presence bit.
func knownHeaderBit(key string) (uint, bool) {
	switch key {
	case JSMsgId:
		return 0, true
	case JSExpectedStream:
		return 1, true
	case JSExpectedLastSeq:
		return 2, true
	case JSExpectedLastSubjSeq:
		return 3, true
	case JSExpectedLastSubjSeqSubj:
		return 4, true
	case JSExpectedLastMsgId:
		return 5, true
	case JSMsgRollup:
		return 6, true
	case JSMessageTTL:
		return 7, true
	case JSMessageIncr:
		return 8, true
	case JSMessageCounterSources:
		return 9, true
	case JSStreamSource:
		return 10, true
	case JSBatchId:
		return 11, true
	case JSBatchSeq:
		return 12, true
	case JSBatchCommit:
		return 13, true
	case JSSchedulePattern:
		return 14, true
	case JSScheduleTimeZone:
		return 15, true
	case JSScheduleTTL:
		return 16, true
	case JSScheduleTarget:
		return 17, true
	case JSScheduleSource:
		return 18, true
	case JSScheduleRollup:
		return 19, true
	case JSScheduleNext:
		return 20, true
	case JSScheduler:
		return 21, true
	case JSRequiredApiLevel:
		return 22, true
	}
	return 0, false
}
