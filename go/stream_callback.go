package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
)

// streamCallbackEnvelope keeps only slices into the host-owned callback while
// cliproxyPluginCall is active. Any field that crosses the callback boundary
// is copied into a compact request or usage record first.
type streamCallbackEnvelope struct {
	data               []byte
	sourceFormat       string
	model              string
	requestedModel     string
	correlationID      string
	requestHeadersRaw  []byte
	responseHeadersRaw []byte
	originalRequestRaw []byte
	requestBodyRaw     []byte
	body               []byte
	statusCode         int
	metadataRaw        []byte
	historyRaw         []byte
	historyCandidate   []byte
	chunkIndex         int
	bodyHasUsage       bool
	historyHasUsage    bool
	terminal           bool
	bodySeen           bool
}

var streamDecodeBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 32<<10)
	},
}

const maxStreamDecodePoolBytes = 256 << 10

// Non-terminal history probing is a compatibility aid for small callbacks.
// Large cumulative histories are deferred to the terminal scan so an ordinary
// chunk remains close to O(current Body) instead of repeatedly walking MiBs.
const maxNonTerminalHistoryProbeBytes = 512 << 10

func acquireStreamDecodeBuffer(size int) []byte {
	buffer := streamDecodeBufferPool.Get().([]byte)
	if cap(buffer) < size {
		return make([]byte, size)
	}
	return buffer[:size]
}

func releaseStreamDecodeBuffer(buffer []byte) {
	if cap(buffer) == 0 || cap(buffer) > maxStreamDecodePoolBytes {
		return
	}
	streamDecodeBufferPool.Put(buffer[:cap(buffer)])
}

// inspectStreamCallbackEnvelope reads only enough of a stream callback to
// decide whether it is a settlement. Body is normally before HistoryChunks in
// the host ABI, so ordinary content chunks return without visiting the large
// cumulative history or decoding prompt/request fields.
func inspectStreamCallbackEnvelope(data []byte) (streamCallbackEnvelope, error) {
	envelope := streamCallbackEnvelope{data: data}
	if len(bytes.TrimSpace(data)) == 0 {
		return envelope, fmt.Errorf("stream callback is empty")
	}
	stopAfterBody := false
	err := scanStreamJSONObject(data, func(key string, rawValue []byte) (bool, error) {
		normalized := normalizeResponseStreamFieldName(key)
		if normalized == "body" {
			body, err := decodeOwnedBase64JSONValue(rawValue)
			if err != nil {
				return false, err
			}
			envelope.bodySeen = true
			envelope.bodyHasUsage = responseBodyMayContainUsage(body)
			envelope.terminal = streamBodyIsTerminal(body)
			if envelope.bodyHasUsage || envelope.terminal {
				envelope.body = body
				stopAfterBody = false
			} else {
				// HistoryChunks is cumulative on some hosts and can be several
				// megabytes. Probe only its newest JSON string here. If it does
				// not carry usage, stop before the scanner visits that array.
				var candidate []byte
				var candidateErr error
				if len(envelope.data) <= maxNonTerminalHistoryProbeBytes {
					candidate, candidateErr = probeLatestHistoryUsage(envelope.data)
				}
				if candidateErr != nil {
					releaseStreamDecodeBuffer(body)
					return false, candidateErr
				}
				if len(candidate) > 0 {
					envelope.historyCandidate = candidate
					envelope.historyHasUsage = true
					releaseStreamDecodeBuffer(body)
					stopAfterBody = false
				} else {
					releaseStreamDecodeBuffer(body)
					stopAfterBody = true
				}
			}
			if stopAfterBody {
				return true, nil
			}
			return false, nil
		}
		if stopAfterBody {
			return true, nil
		}
		if normalized == "historychunks" {
			// Keep a view into the host-owned buffer. It is decoded only after
			// settlement is known and never crosses the callback boundary raw.
			envelope.historyRaw = rawValue
			return false, nil
		}
		return decodeStreamEnvelopeField(&envelope, normalized, rawValue)
	})
	if err != nil {
		return envelope, err
	}
	if !envelope.bodySeen {
		return envelope, fmt.Errorf("stream callback has no body field")
	}
	return envelope, nil
}

func decodeStreamEnvelopeField(envelope *streamCallbackEnvelope, normalized string, raw []byte) (bool, error) {
	if envelope == nil {
		return false, fmt.Errorf("stream callback envelope is nil")
	}
	switch normalized {
	case "sourceformat":
		return false, decodeJSONString(raw, &envelope.sourceFormat)
	case "model":
		return false, decodeJSONString(raw, &envelope.model)
	case "requestedmodel":
		return false, decodeJSONString(raw, &envelope.requestedModel)
	case "responseid", "requestid", "streamid":
		if envelope.correlationID == "" {
			return false, decodeJSONString(raw, &envelope.correlationID)
		}
		return false, nil
	case "requestheaders":
		envelope.requestHeadersRaw = raw
		return false, nil
	case "responseheaders":
		envelope.responseHeadersRaw = raw
		return false, nil
	case "originalrequest":
		envelope.originalRequestRaw = raw
		return false, nil
	case "requestbody":
		envelope.requestBodyRaw = raw
		return false, nil
	case "statuscode":
		return false, json.Unmarshal(raw, &envelope.statusCode)
	case "metadata":
		envelope.metadataRaw = raw
		return false, nil
	case "chunkindex":
		return false, json.Unmarshal(raw, &envelope.chunkIndex)
	default:
		return false, nil
	}
}

// decodeStreamSettlement converts the selected callback fields into an owned
// request. History is decoded only for a terminal callback whose current Body
// did not contain usage.
func decodeStreamSettlement(envelope streamCallbackEnvelope) (ResponseStreamChunkRequest, bool, int, error) {
	var requestHeaders map[string][]string
	var responseHeaders map[string][]string
	var originalRequest []byte
	var requestBody []byte
	var metadata map[string]any
	if envelope.bodyHasUsage || envelope.historyHasUsage || envelope.terminal {
		if len(envelope.requestHeadersRaw) > 0 {
			if err := json.Unmarshal(envelope.requestHeadersRaw, &requestHeaders); err != nil {
				return ResponseStreamChunkRequest{}, false, 0, err
			}
		}
		if len(envelope.responseHeadersRaw) > 0 {
			if err := json.Unmarshal(envelope.responseHeadersRaw, &responseHeaders); err != nil {
				return ResponseStreamChunkRequest{}, false, 0, err
			}
		}
		if len(envelope.metadataRaw) > 0 {
			if err := json.Unmarshal(envelope.metadataRaw, &metadata); err != nil {
				return ResponseStreamChunkRequest{}, false, 0, err
			}
		}
		// Model, requested model, and service tier are normally already
		// present in compact envelope fields. Decode at most one request
		// payload, and only when a fallback value is actually needed. This
		// keeps large prompt buffers out of the common usage settlement path.
		needsRequestPayload := envelope.model == "" || envelope.requestedModel == "" || metadataString(metadata, "service_tier") == ""
		if needsRequestPayload {
			requestRaw := envelope.requestBodyRaw
			if len(requestRaw) == 0 {
				requestRaw = envelope.originalRequestRaw
			}
			if len(requestRaw) > 0 {
				decoded, err := decodeOwnedBase64JSONValue(requestRaw)
				if err != nil {
					return ResponseStreamChunkRequest{}, false, 0, err
				}
				if len(envelope.requestBodyRaw) > 0 {
					requestBody = decoded
				} else {
					originalRequest = decoded
				}
			}
		}
	}
	req := ResponseStreamChunkRequest{
		ResponseInterceptRequest: ResponseInterceptRequest{
			SourceFormat:    envelope.sourceFormat,
			Model:           envelope.model,
			RequestedModel:  envelope.requestedModel,
			correlationID:   envelope.correlationID,
			Stream:          true,
			RequestHeaders:  requestHeaders,
			ResponseHeaders: responseHeaders,
			OriginalRequest: originalRequest,
			RequestBody:     requestBody,
			Body:            envelope.body,
			StatusCode:      envelope.statusCode,
			Metadata:        metadata,
		},
		ChunkIndex: envelope.chunkIndex,
	}
	if envelope.bodyHasUsage {
		return req, true, 0, nil
	}
	if envelope.historyHasUsage && len(envelope.historyCandidate) > 0 {
		req.HistoryChunks = [][]byte{envelope.historyCandidate}
		return req, true, len(envelope.historyCandidate), nil
	}
	if !envelope.terminal || len(envelope.historyRaw) == 0 {
		return req, false, 0, nil
	}
	history, hasUsage, decodedBytes, err := decodeTerminalHistoryUsage(envelope.historyRaw)
	if err != nil {
		return req, false, decodedBytes, err
	}
	req.HistoryChunks = history
	return req, hasUsage, decodedBytes, nil
}

func streamBodyIsTerminal(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if bytes.Equal(trimmed, []byte("[DONE]")) || bytes.Equal(trimmed, []byte("data: [DONE]")) {
		return true
	}
	return bytes.Contains(body, []byte("response.completed")) ||
		bytes.Contains(body, []byte("response.failed")) ||
		bytes.Contains(body, []byte("response.incomplete")) ||
		bytes.Contains(body, []byte("message_stop")) ||
		streamBodyHasStopReason(body)
}

func decodeTerminalHistoryUsage(raw []byte) ([][]byte, bool, int, error) {
	var latest []byte
	decodedBytes := 0
	err := scanStreamJSONArrayReverse(raw, func(element []byte) (bool, error) {
		decoded, err := decodeBase64JSONValue(element)
		if err != nil {
			// A terminal history can contain non-payload sentinels. Ignore
			// malformed individual items and keep searching older entries.
			return false, nil
		}
		decodedBytes += len(decoded)
		if !responseBodyMayContainUsage(decoded) || responseStreamPayloadIsIgnored(decoded) {
			releaseStreamDecodeBuffer(decoded)
			return false, nil
		}
		latest = append([]byte(nil), decoded...)
		releaseStreamDecodeBuffer(decoded)
		return true, nil
	})
	if err != nil {
		return nil, false, decodedBytes, err
	}
	if len(latest) == 0 {
		return nil, false, decodedBytes, nil
	}
	return [][]byte{latest}, true, decodedBytes, nil
}

func streamBodyHasStopReason(body []byte) bool {
	marker := []byte(`"stop_reason"`)
	for offset := 0; ; {
		index := bytes.Index(body[offset:], marker)
		if index < 0 {
			return false
		}
		index += offset + len(marker)
		index = skipStreamJSONSpace(body, index)
		if index >= len(body) || body[index] != ':' {
			offset = index
			continue
		}
		index = skipStreamJSONSpace(body, index+1)
		if index >= len(body) {
			return false
		}
		if bytes.HasPrefix(body[index:], []byte("null")) {
			offset = index + len("null")
			continue
		}
		return true
	}
}

// probeLatestHistoryUsage checks only the newest HistoryChunks element. This
// preserves history-only settlement for hosts that emit the final usage item
// there without decoding the cumulative array on every ordinary callback.
func probeLatestHistoryUsage(data []byte) ([]byte, error) {
	raw, ok := findStreamCallbackField(data, "historychunks")
	if !ok {
		return nil, nil
	}
	element, ok := latestStreamJSONArrayString(raw)
	if !ok {
		return nil, nil
	}
	decoded, err := decodeBase64JSONValue(element)
	if err != nil {
		return nil, nil
	}
	if !responseBodyMayContainUsage(decoded) || responseStreamPayloadIsIgnored(decoded) {
		releaseStreamDecodeBuffer(decoded)
		return nil, nil
	}
	owned := append([]byte(nil), decoded...)
	releaseStreamDecodeBuffer(decoded)
	return owned, nil
}

func findStreamCallbackField(data []byte, normalized string) ([]byte, bool) {
	if normalized != "historychunks" {
		return nil, false
	}
	for _, name := range streamHistoryFieldNames {
		quoted := []byte(`"` + name + `"`)
		position := len(data)
		for position > 0 {
			index := bytes.LastIndex(data[:position], quoted)
			if index < 0 {
				break
			}
			valueStart := skipStreamJSONSpace(data, index+len(quoted))
			if valueStart < len(data) && data[valueStart] == ':' {
				valueStart = skipStreamJSONSpace(data, valueStart+1)
				if normalized == "historychunks" && valueStart < len(data) && data[valueStart] == '[' {
					if close := bytes.IndexByte(data[valueStart:], ']'); close >= 0 {
						return data[valueStart : valueStart+close+1], true
					}
					return nil, false
				}
				valueEnd, err := scanStreamJSONValue(data, valueStart)
				if err == nil {
					return data[valueStart:valueEnd], true
				}
			}
			position = index
		}
	}
	return nil, false
}

var streamHistoryFieldNames = [...]string{"HistoryChunks", "history_chunks", "historyChunks"}

func latestStreamJSONArrayString(raw []byte) ([]byte, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil, false
	}
	position := len(raw) - 2
	for position >= 0 && isStreamJSONSpace(raw[position]) {
		position--
	}
	if position < 0 || raw[position] != '"' {
		return nil, false
	}
	endQuote := position
	for position--; position >= 0; position-- {
		if raw[position] != '"' || streamJSONQuoteIsEscaped(raw, position) {
			continue
		}
		start := position
		before := start - 1
		for before >= 0 && isStreamJSONSpace(raw[before]) {
			before--
		}
		if before < 0 || (raw[before] != '[' && raw[before] != ',') {
			continue
		}
		return raw[start : endQuote+1], true
	}
	return nil, false
}

func streamJSONQuoteIsEscaped(data []byte, position int) bool {
	backslashes := 0
	for index := position - 1; index >= 0 && data[index] == '\\'; index-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func decodeOwnedBase64JSONValue(raw []byte) ([]byte, error) {
	decoded, err := decodeBase64JSONValue(raw)
	if err != nil {
		return nil, err
	}
	owned := append([]byte(nil), decoded...)
	releaseStreamDecodeBuffer(decoded)
	return owned, nil
}

func decodeBase64JSONValue(raw []byte) ([]byte, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return nil, fmt.Errorf("expected Base64 JSON string")
	}
	encoded := raw[1 : len(raw)-1]
	if bytes.Contains(encoded, []byte{'\\'}) {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		encoded = []byte(value)
	}
	decodedSize := base64.StdEncoding.DecodedLen(len(encoded))
	buffer := acquireStreamDecodeBuffer(decodedSize)
	n, err := base64.StdEncoding.Decode(buffer, encoded)
	if err != nil {
		releaseStreamDecodeBuffer(buffer)
		return nil, err
	}
	return buffer[:n], nil
}

func decodeJSONString(raw []byte, target *string) error {
	if target == nil {
		return nil
	}
	return json.Unmarshal(raw, target)
}

func scanStreamJSONObject(data []byte, visit func(string, []byte) (bool, error)) error {
	position := skipStreamJSONSpace(data, 0)
	if position >= len(data) || data[position] != '{' {
		return fmt.Errorf("stream callback is not a JSON object")
	}
	position++
	for {
		position = skipStreamJSONSpace(data, position)
		if position >= len(data) {
			return fmt.Errorf("unterminated stream callback object")
		}
		if data[position] == '}' {
			return nil
		}
		keyEnd, err := scanStreamJSONString(data, position)
		if err != nil {
			return err
		}
		var key string
		if err := json.Unmarshal(data[position:keyEnd], &key); err != nil {
			return err
		}
		position = skipStreamJSONSpace(data, keyEnd)
		if position >= len(data) || data[position] != ':' {
			return fmt.Errorf("stream callback object is missing a colon")
		}
		position = skipStreamJSONSpace(data, position+1)
		valueEnd, err := scanStreamJSONValue(data, position)
		if err != nil {
			return err
		}
		stop, err := visit(key, data[position:valueEnd])
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		position = skipStreamJSONSpace(data, valueEnd)
		if position >= len(data) {
			return fmt.Errorf("unterminated stream callback object")
		}
		if data[position] == '}' {
			return nil
		}
		if data[position] != ',' {
			return fmt.Errorf("stream callback object is missing a comma")
		}
		position++
	}
}

func scanStreamJSONArray(data []byte, visit func([]byte) (bool, error)) error {
	position := skipStreamJSONSpace(data, 0)
	if position >= len(data) || data[position] != '[' {
		return fmt.Errorf("stream history is not an array")
	}
	position++
	for {
		position = skipStreamJSONSpace(data, position)
		if position >= len(data) {
			return fmt.Errorf("unterminated stream history array")
		}
		if data[position] == ']' {
			return nil
		}
		valueEnd, err := scanStreamJSONValue(data, position)
		if err != nil {
			return err
		}
		stop, err := visit(data[position:valueEnd])
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		position = skipStreamJSONSpace(data, valueEnd)
		if position >= len(data) {
			return fmt.Errorf("unterminated stream history array")
		}
		if data[position] == ']' {
			return nil
		}
		if data[position] != ',' {
			return fmt.Errorf("stream history array is missing a comma")
		}
		position++
	}
}

func scanStreamJSONArrayReverse(data []byte, visit func([]byte) (bool, error)) error {
	data = bytes.TrimSpace(data)
	if len(data) < 2 || data[0] != '[' || data[len(data)-1] != ']' {
		return fmt.Errorf("stream history is not an array")
	}
	position := len(data) - 2
	for {
		for position >= 0 && isStreamJSONSpace(data[position]) {
			position--
		}
		if position < 0 {
			return fmt.Errorf("unterminated stream history array")
		}
		if data[position] == '[' {
			return nil
		}
		if data[position] != '"' {
			return fmt.Errorf("stream history item is not a JSON string")
		}
		endQuote := position
		startQuote := -1
		for position--; position >= 0; position-- {
			if data[position] == '"' && !streamJSONQuoteIsEscaped(data, position) {
				startQuote = position
				break
			}
		}
		if startQuote < 0 {
			return fmt.Errorf("unterminated stream history string")
		}
		stop, err := visit(data[startQuote : endQuote+1])
		if err != nil || stop {
			return err
		}
		position = startQuote - 1
		for position >= 0 && isStreamJSONSpace(data[position]) {
			position--
		}
		if position < 0 || data[position] == '[' {
			if position < 0 {
				return fmt.Errorf("unterminated stream history array")
			}
			return nil
		}
		if data[position] != ',' {
			return fmt.Errorf("stream history array is missing a comma")
		}
		position--
	}
}

func scanStreamJSONValue(data []byte, position int) (int, error) {
	if position >= len(data) {
		return 0, fmt.Errorf("missing JSON value")
	}
	switch data[position] {
	case '"':
		return scanStreamJSONString(data, position)
	case '{':
		return scanStreamJSONComposite(data, position, '}')
	case '[':
		return scanStreamJSONComposite(data, position, ']')
	default:
		end := position
		for end < len(data) && data[end] != ',' && data[end] != ']' && data[end] != '}' {
			end++
		}
		trimmed := end
		for trimmed > position && isStreamJSONSpace(data[trimmed-1]) {
			trimmed--
		}
		if trimmed == position {
			return 0, fmt.Errorf("empty JSON value")
		}
		return trimmed, nil
	}
}

func scanStreamJSONComposite(data []byte, position int, closeByte byte) (int, error) {
	openByte := data[position]
	position++
	for {
		position = skipStreamJSONSpace(data, position)
		if position >= len(data) {
			return 0, fmt.Errorf("unterminated JSON composite")
		}
		if data[position] == closeByte {
			return position + 1, nil
		}
		if openByte == '{' {
			keyEnd, err := scanStreamJSONString(data, position)
			if err != nil {
				return 0, err
			}
			position = skipStreamJSONSpace(data, keyEnd)
			if position >= len(data) || data[position] != ':' {
				return 0, fmt.Errorf("JSON object is missing a colon")
			}
			position = skipStreamJSONSpace(data, position+1)
		}
		valueEnd, err := scanStreamJSONValue(data, position)
		if err != nil {
			return 0, err
		}
		position = skipStreamJSONSpace(data, valueEnd)
		if position >= len(data) {
			return 0, fmt.Errorf("unterminated JSON composite")
		}
		if data[position] == closeByte {
			return position + 1, nil
		}
		if data[position] != ',' {
			return 0, fmt.Errorf("JSON composite is missing a comma")
		}
		position++
	}
}

func scanStreamJSONString(data []byte, position int) (int, error) {
	if position >= len(data) || data[position] != '"' {
		return 0, fmt.Errorf("expected JSON string")
	}
	for index := position + 1; index < len(data); index++ {
		switch data[index] {
		case '\\':
			index++
		case '"':
			return index + 1, nil
		}
	}
	return 0, fmt.Errorf("unterminated JSON string")
}

func skipStreamJSONSpace(data []byte, position int) int {
	for position < len(data) && isStreamJSONSpace(data[position]) {
		position++
	}
	return position
}

func isStreamJSONSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}
