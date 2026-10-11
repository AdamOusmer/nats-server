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

// strInternMax caps the distinct strings an interner remembers. Past it, new
// strings are allocated as usual, so a fan-out of distinct subjects cannot
// grow it without bound.
const strInternMax = 4096

// strInterner hands out one shared copy per distinct string. It is owned by a
// single goroutine and is not safe for concurrent use.
type strInterner struct {
	m map[string]string
}

// newApplyInterner returns the interner for a stream's apply loop, or nil
// when NATS_BENCH_INTERN is off.
func newApplyInterner() *strInterner {
	if !benchIntern {
		return nil
	}
	return &strInterner{m: make(map[string]string, 64)}
}

// str returns an interned copy of b. A nil interner just allocates.
func (in *strInterner) str(b []byte) string {
	if in == nil {
		return string(b)
	}
	if s, ok := in.m[string(b)]; ok {
		return s
	}
	s := string(b)
	if len(in.m) < strInternMax {
		in.m[s] = s
	}
	return s
}
