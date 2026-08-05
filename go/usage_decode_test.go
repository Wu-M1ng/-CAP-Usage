package main

import (
	"bytes"
	"testing"
)

func TestForEachSSEDataKeepsIndependentLines(t *testing.T) {
	payload := bytes.Join([][]byte{
		[]byte(`data: {"usage":{"input_tokens":7}}`),
		[]byte(`data: {"usage":{"output_tokens":3}}`),
		[]byte(`data: [DONE]`),
	}, []byte{'\n', '\n'})
	var got []string
	forEachSSEData(payload, func(data []byte) bool {
		got = append(got, string(data))
		return true
	})
	if len(got) != 2 || got[0] != `{"usage":{"input_tokens":7}}` || got[1] != `{"usage":{"output_tokens":3}}` {
		t.Fatalf("SSE data = %#v, want two independent JSON lines", got)
	}
}

func TestDecodeUsagePayloadOpenAIResponses(t *testing.T) {
	payload := `data: {"type":"response.completed","response":{"id":"resp-1","model":"gpt-5.6","usage":{"input_tokens":2459,"output_tokens":271,"total_tokens":2730,"input_tokens_details":{"cached_tokens":147200}}}}`
	decoded, ok := decodeUsagePayload([]byte(payload), usageDecodeStream)
	if !ok {
		t.Fatal("OpenAI Responses usage was not decoded")
	}
	if decoded.model != "gpt-5.6" || decoded.correlationID != "resp-1" || decoded.detail.InputTokens != 2459 || decoded.detail.OutputTokens != 271 || decoded.detail.TotalTokens != 2730 || decoded.detail.CacheReadTokens != 147200 {
		t.Fatalf("decoded OpenAI Responses usage = %#v, want typed token parts and response id", decoded)
	}
}

func TestDecodeUsagePayloadAnthropicCacheReadWrite(t *testing.T) {
	payload := `{"type":"message_delta","usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":40,"cache_creation_input_tokens":8}}`
	decoded, ok := decodeUsagePayload([]byte(payload), usageDecodeStream)
	if !ok {
		t.Fatal("Anthropic usage was not decoded")
	}
	if decoded.detail.InputTokens != 100 || decoded.detail.OutputTokens != 20 || decoded.detail.CacheReadTokens != 40 || decoded.detail.CacheCreationTokens != 8 {
		t.Fatalf("decoded Anthropic usage = %#v", decoded.detail)
	}
}

func TestDecodeUsagePayloadGeminiUsageMetadata(t *testing.T) {
	decoded, ok := decodeUsagePayload([]byte(`{"model":"gemini-2.5","usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":4,"cachedContentTokenCount":3,"thoughtsTokenCount":2,"totalTokenCount":18}}`), usageDecodeComplete)
	if !ok {
		t.Fatal("Gemini usage metadata was not decoded")
	}
	if decoded.detail.InputTokens != 12 || decoded.detail.OutputTokens != 4 || decoded.detail.CachedTokens != 3 || decoded.detail.CacheReadTokens != 3 || decoded.detail.ReasoningTokens != 2 || decoded.detail.TotalTokens != 18 {
		t.Fatalf("decoded Gemini usage = %#v", decoded.detail)
	}
}

func TestDecodeUsagePayloadIgnoresClaudeMessageStartUsageInStreamMode(t *testing.T) {
	decoded, ok := decodeUsagePayload([]byte(`{"type":"message_start","message":{"model":"claude-sonnet","usage":{"input_tokens":1200,"output_tokens":1}}}`), usageDecodeStream)
	if ok || decoded.detail != (UsageDetail{}) {
		t.Fatalf("message_start usage = %#v/%v, want ignored", decoded, ok)
	}
}

func TestDecodeUsagePayloadKeepsLatestCompleteIndependentSSEUsage(t *testing.T) {
	payload := bytes.Join([][]byte{
		[]byte(`data: {"usage":{"input_tokens":10,"output_tokens":1}}`),
		[]byte(`data: {"usage":{"input_tokens":20,"output_tokens":7,"total_tokens":27}}`),
	}, []byte{'\n', '\n'})
	decoded, ok := decodeUsagePayload(payload, usageDecodeStream)
	if !ok || decoded.detail.InputTokens != 20 || decoded.detail.OutputTokens != 7 || decoded.detail.TotalTokens != 27 {
		t.Fatalf("latest usage = %#v/%v, want 20/7/27", decoded, ok)
	}
}

func TestDecodeUsagePayloadAcceptsNumericCorrelationID(t *testing.T) {
	decoded, ok := decodeUsagePayload([]byte(`{"id":12345,"usage":{"input_tokens":4,"output_tokens":2,"total_tokens":6}}`), usageDecodeComplete)
	if !ok || decoded.correlationID != "12345" {
		t.Fatalf("numeric correlation id = %#v/%v, want 12345/true", decoded, ok)
	}
}
