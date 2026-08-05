package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func streamJSONBase64(value string) string {
	return base64.StdEncoding.EncodeToString([]byte(value))
}

func TestInspectStreamCallbackSkipsLargeHistoryOnOrdinaryChunk(t *testing.T) {
	body := streamJSONBase64(`data: {"choices":[{"delta":{"content":"hello"}}]}`)
	raw := []byte(`{"Body":"` + body + `","HistoryChunks":["%%%not-base64%%%"]}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if envelope.bodyHasUsage || envelope.terminal {
		t.Fatalf("ordinary envelope = %#v, want no usage or terminal", envelope)
	}
}

func TestInspectStreamCallbackDetectsCurrentBodyUsage(t *testing.T) {
	body := streamJSONBase64(`data: {"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)
	raw := []byte(`{"Body":"` + body + `","Model":"gpt-5.5","StatusCode":200}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if !envelope.bodyHasUsage || envelope.terminal || envelope.model != "gpt-5.5" {
		t.Fatalf("usage envelope = %#v, want current usage and model", envelope)
	}
	req, hasUsage, historyBytes, err := decodeStreamSettlement(envelope)
	if err != nil {
		t.Fatalf("decodeStreamSettlement() error = %v", err)
	}
	if !hasUsage || historyBytes != 0 || len(req.Body) == 0 {
		t.Fatalf("settlement = usage %v history bytes %d body %d, want usage/body and no history scan", hasUsage, historyBytes, len(req.Body))
	}
}

func TestInspectStreamCallbackDetectsUsageInHistoryTailWithoutTerminal(t *testing.T) {
	body := streamJSONBase64(`data: {"choices":[{"delta":{"content":"hello"}}]}`)
	historyUsage := streamJSONBase64(`data: {"usage":{"prompt_tokens":2459,"completion_tokens":271,"total_tokens":2730}}`)
	raw := []byte(`{"Body":"` + body + `","Model":"gpt-5.6-luna","HistoryChunks":["` + historyUsage + `"],"ResponseID":"resp-1"}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if envelope.bodyHasUsage || !envelope.historyHasUsage || envelope.terminal {
		t.Fatalf("history envelope = %#v, want history usage without terminal", envelope)
	}
	req, hasUsage, _, err := decodeStreamSettlement(envelope)
	if err != nil {
		t.Fatalf("decodeStreamSettlement() error = %v", err)
	}
	if !hasUsage || len(req.HistoryChunks) != 1 || req.correlationID != "resp-1" {
		t.Fatalf("history settlement = %#v/%v, want one usage chunk and response id", req, hasUsage)
	}
}

func TestInspectStreamCallbackDetectsOpenAIResponsesTerminal(t *testing.T) {
	body := streamJSONBase64(`data: [DONE]`)
	history := streamJSONBase64(`data: {"response":{"usage":{"input_tokens":100,"output_tokens":20,"total_tokens":120}}}`)
	raw := []byte(`{"Body":"` + body + `","HistoryChunks":["` + history + `"],"StatusCode":200}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if !envelope.terminal || envelope.bodyHasUsage || len(envelope.historyRaw) == 0 {
		t.Fatalf("terminal envelope = %#v, want terminal history", envelope)
	}
	_, hasUsage, decodedBytes, err := decodeStreamSettlement(envelope)
	if err != nil {
		t.Fatalf("decodeStreamSettlement() error = %v", err)
	}
	if !hasUsage || decodedBytes == 0 {
		t.Fatalf("terminal history settlement = usage %v bytes %d, want usage", hasUsage, decodedBytes)
	}
}

func TestInspectStreamCallbackDetectsAnthropicMessageStop(t *testing.T) {
	body := streamJSONBase64(`event: message_stop`)
	raw := []byte(`{"body":"` + body + `","history_chunks":[]}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if !envelope.terminal {
		t.Fatalf("Anthropic envelope = %#v, want terminal", envelope)
	}
}

func TestInspectStreamCallbackDetectsAnthropicStopReason(t *testing.T) {
	body := streamJSONBase64(`event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}`)
	raw := []byte(`{"body":"` + body + `"}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if !envelope.terminal {
		t.Fatalf("stop-reason envelope = %#v, want terminal", envelope)
	}
}

func TestInspectStreamCallbackAcceptsPascalSnakeAndCamelFields(t *testing.T) {
	body := streamJSONBase64(`data: {"usage":{"total_tokens":12}}`)
	values := map[string]any{
		"body":           body,
		"source_format":  "openai-responses",
		"requestedModel": "gpt-requested",
		"model":          "gpt-5.5",
		"status_code":    200,
		"chunkIndex":     7,
	}
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	if envelope.sourceFormat != "openai-responses" || envelope.model != "gpt-5.5" || envelope.requestedModel != "gpt-requested" || envelope.chunkIndex != 7 {
		t.Fatalf("normalized envelope = %#v", envelope)
	}
}

func TestInspectStreamCallbackKeepsSettlementFieldsSeenBeforeBody(t *testing.T) {
	body := streamJSONBase64(`data: {"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)
	request := streamJSONBase64(`{"model":"gpt-requested"}`)
	raw := []byte(`{"Model":"gpt-5.5","RequestedModel":"gpt-requested","ResponseID":"resp-before-body","Metadata":{"upstream_provider":"openai"},"RequestBody":"` + request + `","Body":"` + body + `","StatusCode":200}`)
	envelope, err := inspectStreamCallbackEnvelope(raw)
	if err != nil {
		t.Fatalf("inspectStreamCallbackEnvelope() error = %v", err)
	}
	req, hasUsage, _, err := decodeStreamSettlement(envelope)
	if err != nil {
		t.Fatalf("decodeStreamSettlement() error = %v", err)
	}
	if !hasUsage || req.Model != "gpt-5.5" || req.RequestedModel != "gpt-requested" || req.correlationID != "resp-before-body" {
		t.Fatalf("settlement identity = %#v/%v, want model/request/id and usage", req, hasUsage)
	}
	if got := metadataString(req.Metadata, "upstream_provider"); got != "openai" {
		t.Fatalf("settlement metadata = %q, want openai", got)
	}
	if len(req.RequestBody) == 0 {
		t.Fatal("settlement request body was lost when it preceded Body")
	}
}

func TestInspectStreamCallbackRejectsMalformedEnvelope(t *testing.T) {
	if _, err := inspectStreamCallbackEnvelope([]byte(`{"Body":"`)); err == nil {
		t.Fatal("malformed callback unexpectedly accepted")
	}
}

func TestTerminalHistorySearchStopsAtLatestValidUsage(t *testing.T) {
	history := []string{
		`data: {"usage":{"total_tokens":10}}`,
		`data: {"choices":[{"delta":{"content":"x"}}]}`,
		`data: {"usage":{"total_tokens":20}}`,
	}
	encoded := make([]string, 0, len(history))
	for _, item := range history {
		encoded = append(encoded, streamJSONBase64(item))
	}
	raw, err := json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}
	chunks, ok, _, err := decodeTerminalHistoryUsage(raw)
	if err != nil {
		t.Fatalf("decodeTerminalHistoryUsage() error = %v", err)
	}
	if !ok || len(chunks) != 1 || string(chunks[0]) != history[2] {
		t.Fatalf("latest history = %q/%v, want %q/true", chunks, ok, history[2])
	}
}
