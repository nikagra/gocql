//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/streams"
)

// segmentReader is a minimal ConnReader backed by an in-memory byte stream,
// used to drive the v5 segment reassembly path without a real socket.
type segmentReader struct {
	r *bytes.Reader
}

func newSegmentReader(b []byte) *segmentReader {
	return &segmentReader{r: bytes.NewReader(b)}
}

func (s *segmentReader) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *segmentReader) Close() error               { return nil }
func (s *segmentReader) RemoteAddr() net.Addr       { return nil }
func (s *segmentReader) SetTimeout(_ time.Duration) {}
func (s *segmentReader) GetTimeout() time.Duration  { return 0 }

// mustUncompressedSegment builds a single uncompressed transport segment
// carrying payload, failing the test on error.
func mustUncompressedSegment(t *testing.T, payload []byte, selfContained bool) []byte {
	t.Helper()
	seg, err := newUncompressedSegment(payload, selfContained)
	if err != nil {
		t.Fatalf("newUncompressedSegment: %v", err)
	}
	return seg
}

func TestReadContinuationSegmentReturnsPayload(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), false)
	c := &Conn{r: newSegmentReader(seg)}

	payload, err := c.readContinuationSegment()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := string(payload); got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestReadContinuationSegmentRejectsSelfContained(t *testing.T) {
	seg := mustUncompressedSegment(t, []byte("hello"), true)
	c := &Conn{r: newSegmentReader(seg)}

	_, err := c.readContinuationSegment()
	if err == nil || !strings.Contains(err.Error(), "expected a continuation") {
		t.Fatalf("expected self-contained rejection, got %v", err)
	}
}

func TestReadContinuationSegmentRejectsEmptyPayload(t *testing.T) {
	seg := mustUncompressedSegment(t, nil, false)
	c := &Conn{r: newSegmentReader(seg)}

	_, err := c.readContinuationSegment()
	if err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("expected no-progress rejection, got %v", err)
	}
}

// TestRecvSplitFrameRejectsOverlongStream pins that continuation segments
// carrying more bytes than the CQL frame header declared are rejected, rather
// than appended past the size the reassembly buffer was allocated for.
func TestRecvSplitFrameRejectsOverlongStream(t *testing.T) {
	// A header declaring a 4-byte body, so the whole frame is headSize+4 bytes,
	// followed by a continuation segment carrying 8 body bytes.
	header := make([]byte, headSize)
	header[0] = protoVersion5 | protoDirectionMask
	header[8] = 4

	seg := mustUncompressedSegment(t, []byte("01234567"), false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), header)
	if err == nil || !strings.Contains(err.Error(), "exceeds its declared length") {
		t.Fatalf("expected over-long rejection, got %v", err)
	}
}

// TestRecvSplitFrameAllocatesExactlyOneFrameBuffer pins the reassembly buffer
// against the two allocation regressions it is shaped to avoid: growing it as
// payloads arrive (a bytes.Buffer reaches ~2x the frame size), and then copying
// the assembled frame into the read framer instead of handing it over. Both are
// invisible in the output but double or triple the memory a single large response
// occupies.
func TestRecvSplitFrameAllocatesExactlyOneFrameBuffer(t *testing.T) {
	const bodyLen = 3 * maxSegmentPayloadSize

	// Enough streams to also let the framer pool warm up.
	const runs = 4
	body := bytes.Repeat([]byte{0x5A}, bodyLen)
	frame := make([]byte, headSize)
	frame[0] = protoVersion5 | protoDirectionMask
	// Stream -1 is the event stream: with no session attached, processFrame parses
	// the frame and drops it, which is all this test needs. A READY frame carries
	// no body fields, so the filler body is never inspected.
	binary.BigEndian.PutUint16(frame[2:4], uint16(0xFFFF))
	frame[4] = byte(frm.OpReady)
	binary.BigEndian.PutUint32(frame[5:headSize], uint32(bodyLen))
	frame = append(frame, body...)

	var stream []byte
	for src := frame; len(src) > 0; {
		n := min(len(src), maxSegmentPayloadSize)
		stream = append(stream, mustUncompressedSegment(t, src[:n], false)...)
		src = src[n:]
	}

	c := &Conn{
		r:       newSegmentReader(bytes.Repeat(stream, runs)),
		streams: streams.New(),
		logger:  &defaultLogger{},
	}
	c.initFramerCache()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		// Stream 0 is unused, so processFrame parses and drops the frame; that is
		// enough to exercise reassembly and the framer hand-over.
		if err := c.recvSegment(context.Background()); err != nil {
			t.Fatalf("run %d: recvSegment: %v", i, err)
		}
	}
	runtime.ReadMemStats(&after)

	// One buffer per frame, sized to the frame: anything much above that means
	// either the buffer was grown into or the frame was copied again.
	allocated := after.TotalAlloc - before.TotalAlloc
	if limit := uint64(runs) * uint64(len(frame)) * 3 / 2; allocated > limit {
		t.Errorf("reassembling %d frames of %d bytes allocated %d bytes, want at most %d",
			runs, len(frame), allocated, limit)
	}
}

// TestRecvSplitFrameRejectsOversizedLength drives recvSplitFrame with a CQL
// frame header declaring a body length beyond maxFrameSize. The declared
// length is rejected before any large allocation and before processFrame.
func TestRecvSplitFrameRejectsOversizedLength(t *testing.T) {
	// Build a 9-byte CQL frame header (v5 response) whose length field is
	// maxFrameSize+1, then wrap it in a single non-self-contained segment.
	header := make([]byte, 9)
	header[0] = protoVersion5 | protoDirectionMask // version (response)
	header[1] = 0                                  // flags
	// header[2:4] stream, header[4] opcode left zero
	oversized := uint32(maxFrameSize + 1)
	header[5] = byte(oversized >> 24)
	header[6] = byte(oversized >> 16)
	header[7] = byte(oversized >> 8)
	header[8] = byte(oversized)

	seg := mustUncompressedSegment(t, header, false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "invalid frame body length") {
		t.Fatalf("expected oversized-length rejection, got %v", err)
	}
}

// TestRecvSplitFrameRejectsTruncatedHeaderStream ensures the header-accumulation
// loop terminates (with an error) when the peer stops sending before even the
// 9-byte CQL header is complete, rather than looping forever.
func TestRecvSplitFrameRejectsTruncatedHeaderStream(t *testing.T) {
	// A single 4-byte continuation segment, then EOF: fewer than the 9 header
	// bytes recvSplitFrame needs.
	seg := mustUncompressedSegment(t, []byte("abcd"), false)
	c := &Conn{r: newSegmentReader(seg)}

	err := c.recvSplitFrame(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error on truncated header stream, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("continuation segment header")) &&
		err != io.EOF {
		t.Fatalf("expected read failure after truncation, got %v", err)
	}
}
