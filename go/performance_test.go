package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestRuntimeStatusReportsBoundedStreamMetrics(t *testing.T) {
	statistics := NewRequestStatistics()
	statistics.RecordStreamCallbackObservation(streamCallbackObservation{
		inputBytes: 100,
		fastPath:   true,
		duration:   2 * time.Millisecond,
	})
	statistics.RecordStreamCallbackObservation(streamCallbackObservation{
		inputBytes:          300,
		bodyBytesDecoded:    80,
		historyBytesDecoded: 120,
		settlement:          true,
		terminalHistoryScan: true,
		duration:            4 * time.Millisecond,
	})

	stream := statistics.RuntimeStatus().Stream
	if stream.Callbacks != 2 || stream.FastPathCallbacks != 1 || stream.SettlementCallbacks != 1 || stream.TerminalHistoryScans != 1 {
		t.Fatalf("stream counters = %#v, want callbacks=2 fast=1 settlement=1 history=1", stream)
	}
	if stream.InputBytes != 400 || stream.BodyBytesDecoded != 80 || stream.HistoryBytesDecoded != 120 {
		t.Fatalf("stream byte counters = %#v, want input/body/history=400/80/120", stream)
	}
	if stream.CallbackDurationMsAvg != 3 || stream.CallbackDurationMsMax != 4 {
		t.Fatalf("stream duration metrics = %#v, want avg/max=3/4", stream)
	}
}

type streamBenchmarkFixture struct {
	ordinary []byte
	terminal []byte
	current  []byte
}

func newStreamBenchmarkFixture() streamBenchmarkFixture {
	const historyChunkBytes = 32 << 10
	history := make([][]byte, 128)
	for i := range history {
		history[i] = bytes.Repeat([]byte{'x'}, historyChunkBytes)
	}
	history[len(history)-1] = []byte(`data: {"type":"response.completed","usage":{"input_tokens":2459,"output_tokens":271,"total_tokens":2730}}`)

	base := ResponseStreamChunkRequest{
		ResponseInterceptRequest: ResponseInterceptRequest{
			SourceFormat:    "openai-responses",
			Model:           "gpt-5.6-luna",
			RequestedModel:  "gpt-5.6-luna",
			OriginalRequest: bytes.Repeat([]byte{'r'}, 256<<10),
			RequestBody:     bytes.Repeat([]byte{'b'}, 256<<10),
			StatusCode:      200,
			Metadata:        map[string]any{"upstream_provider": "openai", "service_tier": "default"},
		},
		HistoryChunks: history,
		ChunkIndex:    128,
	}
	encode := func(body []byte) []byte {
		copy := base
		copy.Body = body
		raw, err := json.Marshal(copy)
		if err != nil {
			panic(err)
		}
		return raw
	}
	return streamBenchmarkFixture{
		ordinary: encode([]byte(`data: {"choices":[{"delta":{"content":"hello"}}]}`)),
		terminal: encode([]byte(`data: [DONE]`)),
		current:  encode([]byte(`data: {"usage":{"input_tokens":2459,"output_tokens":271,"total_tokens":2730}}`)),
	}
}

func BenchmarkStreamCallbackNoUsage4MiBHistory(b *testing.B) {
	fixture := newStreamBenchmarkFixture()
	b.ReportAllocs()
	b.SetBytes(int64(len(fixture.ordinary)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		envelope, err := inspectStreamCallbackEnvelope(fixture.ordinary)
		if err != nil || envelope.bodyHasUsage || envelope.terminal {
			b.Fatalf("ordinary inspection = %#v/%v", envelope, err)
		}
	}
}

func BenchmarkStreamCallbackHistoryOnlyFinal4MiB(b *testing.B) {
	fixture := newStreamBenchmarkFixture()
	b.ReportAllocs()
	b.SetBytes(int64(len(fixture.terminal)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		envelope, err := inspectStreamCallbackEnvelope(fixture.terminal)
		if err != nil {
			b.Fatal(err)
		}
		if _, hasUsage, _, err := decodeStreamSettlement(envelope); err != nil || !hasUsage {
			b.Fatalf("terminal settlement = %v/%v", hasUsage, err)
		}
	}
}

func BenchmarkStreamCallbackCurrentBodyUsage4MiBHistory(b *testing.B) {
	fixture := newStreamBenchmarkFixture()
	b.ReportAllocs()
	b.SetBytes(int64(len(fixture.current)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		envelope, err := inspectStreamCallbackEnvelope(fixture.current)
		if err != nil {
			b.Fatal(err)
		}
		if _, hasUsage, _, err := decodeStreamSettlement(envelope); err != nil || !hasUsage {
			b.Fatalf("current body settlement = %v/%v", hasUsage, err)
		}
	}
}
