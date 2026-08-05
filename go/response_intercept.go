package main

import (
	"bytes"
	"container/heap"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultUsageFallbackDelay     = 2 * time.Second
	maxUsageFallbackPending       = 4096
	maxUsageFallbackRecent        = 4096
	maxUsageFallbackRetainedBytes = 8 << 20
	// Native usage and response interception are delivered by independent host
	// callbacks. Keep the native observation long enough to cover a slow handoff
	// without making the fallback wait longer before it becomes usable.
	usageFallbackNativeRecentWindow = 5 * time.Second
	usageFallbackLateNativeWindow   = 30 * time.Second
)

var (
	usageFallbackRecordDelay = defaultUsageFallbackDelay
	usageFallbacks           = newUsageFallbackCoordinator()
	authIndexes              = newAuthIndexLearner()
	streamUsages             = newStreamUsageTracker()
)

// authIndexLearner remembers the CPA-computed auth index for each auth ID.
// Native usage records carry both fields, while interceptor metadata only
// carries the auth ID; reusing the learned index keeps fallback records in
// the same credential group as native ones on the dashboard.
type authIndexLearner struct {
	mu      sync.RWMutex
	indexes map[string]string
}

const maxLearnedAuthIndexes = 4096

func newAuthIndexLearner() *authIndexLearner {
	return &authIndexLearner{indexes: make(map[string]string)}
}

func (l *authIndexLearner) Learn(authID, authIndex string) {
	if l == nil {
		return
	}
	key := strings.ToLower(strings.TrimSpace(authID))
	value := strings.TrimSpace(authIndex)
	if key == "" || value == "" || value == safeCredentialIdentity(authID) {
		return
	}
	l.mu.Lock()
	if existing, ok := l.indexes[key]; !ok || existing != value {
		if len(l.indexes) < maxLearnedAuthIndexes || l.indexes[key] != "" {
			l.indexes[key] = value
		}
	}
	l.mu.Unlock()
}

func (l *authIndexLearner) Lookup(authID string) string {
	if l == nil {
		return ""
	}
	key := strings.ToLower(strings.TrimSpace(authID))
	if key == "" {
		return ""
	}
	l.mu.RLock()
	value := l.indexes[key]
	l.mu.RUnlock()
	return value
}

type ResponseInterceptRequest struct {
	SourceFormat    string
	Model           string
	RequestedModel  string
	correlationID   string
	Stream          bool
	RequestHeaders  map[string][]string
	ResponseHeaders map[string][]string
	OriginalRequest []byte
	RequestBody     []byte
	Body            []byte
	StatusCode      int
	Metadata        map[string]any
}

func (r *ResponseInterceptRequest) UnmarshalJSON(data []byte) error {
	return unmarshalResponseInterceptRequest(data, r)
}

type ResponseStreamChunkRequest struct {
	ResponseInterceptRequest
	HistoryChunks [][]byte
	ChunkIndex    int
}

// responseInterceptEnvelope is the owned payload that crosses from the host
// callback into the asynchronous usage processor. The response body and the
// original request are intentionally absent: only the decoded usage fields
// and the metadata needed to build a UsageRecord are retained.
type responseInterceptEnvelope struct {
	req          ResponseInterceptRequest
	decoded      decodedUsage
	usageFound   bool
	bodyHadUsage bool
}

func (r *ResponseStreamChunkRequest) UnmarshalJSON(data []byte) error {
	if err := unmarshalResponseInterceptRequest(data, &r.ResponseInterceptRequest); err != nil {
		return err
	}
	// The host stream-chunk ABI has no Stream field because this callback is
	// intrinsically streaming. Mark it explicitly for persisted throughput
	// calculations and native/fallback metadata reconciliation.
	r.Stream = true
	var wire struct {
		HistoryChunks      [][]byte `json:"HistoryChunks"`
		HistoryChunksSnake [][]byte `json:"history_chunks"`
		ChunkIndex         int      `json:"ChunkIndex"`
		ChunkIndexSnake    int      `json:"chunk_index"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	r.HistoryChunks = wire.HistoryChunks
	if len(r.HistoryChunks) == 0 {
		r.HistoryChunks = wire.HistoryChunksSnake
	}
	r.ChunkIndex = wire.ChunkIndex
	if r.ChunkIndex == 0 {
		r.ChunkIndex = wire.ChunkIndexSnake
	}
	return nil
}

func unmarshalResponseInterceptRequest(data []byte, r *ResponseInterceptRequest) error {
	var wire struct {
		SourceFormat         string              `json:"SourceFormat"`
		SourceFormatSnake    string              `json:"source_format"`
		Model                string              `json:"Model"`
		ModelSnake           string              `json:"model"`
		RequestedModel       string              `json:"RequestedModel"`
		RequestedModelSnake  string              `json:"requested_model"`
		ResponseID           string              `json:"ResponseID"`
		ResponseIDSnake      string              `json:"response_id"`
		ResponseIDCamel      string              `json:"responseId"`
		RequestID            string              `json:"RequestID"`
		RequestIDSnake       string              `json:"request_id"`
		RequestIDCamel       string              `json:"requestId"`
		StreamID             string              `json:"StreamID"`
		StreamIDSnake        string              `json:"stream_id"`
		StreamIDCamel        string              `json:"streamId"`
		Stream               bool                `json:"Stream"`
		StreamSnake          bool                `json:"stream"`
		RequestHeaders       map[string][]string `json:"RequestHeaders"`
		RequestHeadersSnake  map[string][]string `json:"request_headers"`
		ResponseHeaders      map[string][]string `json:"ResponseHeaders"`
		ResponseHeadersSnake map[string][]string `json:"response_headers"`
		OriginalRequest      []byte              `json:"OriginalRequest"`
		OriginalRequestSnake []byte              `json:"original_request"`
		RequestBody          []byte              `json:"RequestBody"`
		RequestBodySnake     []byte              `json:"request_body"`
		Body                 []byte              `json:"Body"`
		BodySnake            []byte              `json:"body"`
		StatusCode           int                 `json:"StatusCode"`
		StatusCodeSnake      int                 `json:"status_code"`
		Metadata             map[string]any      `json:"Metadata"`
		MetadataSnake        map[string]any      `json:"metadata"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	r.SourceFormat = firstNonEmpty(wire.SourceFormat, wire.SourceFormatSnake)
	r.Model = firstNonEmpty(wire.Model, wire.ModelSnake)
	r.RequestedModel = firstNonEmpty(wire.RequestedModel, wire.RequestedModelSnake)
	r.correlationID = firstNonEmpty(
		wire.ResponseID, wire.ResponseIDSnake, wire.ResponseIDCamel,
		wire.RequestID, wire.RequestIDSnake, wire.RequestIDCamel,
		wire.StreamID, wire.StreamIDSnake, wire.StreamIDCamel,
	)
	r.Stream = wire.Stream || wire.StreamSnake
	r.RequestHeaders = firstHeaderMap(wire.RequestHeaders, wire.RequestHeadersSnake)
	r.ResponseHeaders = firstHeaderMap(wire.ResponseHeaders, wire.ResponseHeadersSnake)
	r.OriginalRequest = firstBytes(wire.OriginalRequest, wire.OriginalRequestSnake)
	r.RequestBody = firstBytes(wire.RequestBody, wire.RequestBodySnake)
	r.Body = firstBytes(wire.Body, wire.BodySnake)
	r.StatusCode = wire.StatusCode
	if r.StatusCode == 0 {
		r.StatusCode = wire.StatusCodeSnake
	}
	r.Metadata = wire.Metadata
	if r.Metadata == nil {
		r.Metadata = wire.MetadataSnake
	}
	return nil
}

func handleResponseIntercept(requestBody []byte) ([]byte, error) {
	statistics := stats
	fallbacks := usageFallbacks
	envelope, err := inspectResponseInterceptEnvelope(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response intercept request: %w", err)
	}
	compactBytes := responseInterceptEnvelopeBytes(envelope)
	switch deferUsageCallback(statistics, func() {
		processResponseInterceptEnvelope(statistics, fallbacks, envelope)
	}, compactBytes) {
	case usageCallbackQueued, usageCallbackDropped:
		return okEnvelopeJSON("{}")
	}
	processResponseInterceptEnvelope(statistics, fallbacks, envelope)
	return okEnvelopeJSON("{}")
}

// inspectResponseInterceptEnvelope performs the bounded part of ordinary
// response parsing while the host buffer is still valid. json.Decoder skips
// unrelated large fields, and the response body is decoded only when it has a
// usage marker. The resulting envelope contains no request/response body.
func inspectResponseInterceptEnvelope(data []byte) (responseInterceptEnvelope, error) {
	var envelope responseInterceptEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return envelope, err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return envelope, fmt.Errorf("response intercept request is not a JSON object")
	}
	var requestFields responseRequestMetadata
	var originalFields responseRequestMetadata
	bodySeen := false
	requestSeen := false
	originalSeen := false
	metadataSeen := false
	requestHeadersSeen := false
	responseHeadersSeen := false
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return envelope, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return envelope, fmt.Errorf("response intercept field name is not a string")
		}
		normalized := normalizeResponseStreamFieldName(key)
		switch normalized {
		case "sourceformat":
			if err := decoder.Decode(&envelope.req.SourceFormat); err != nil {
				return envelope, err
			}
		case "model":
			if err := decoder.Decode(&envelope.req.Model); err != nil {
				return envelope, err
			}
		case "requestedmodel":
			if err := decoder.Decode(&envelope.req.RequestedModel); err != nil {
				return envelope, err
			}
		case "responseid", "requestid", "streamid":
			if envelope.req.correlationID == "" {
				if envelope.req.correlationID, err = decodeJSONScalarString(decoder); err != nil {
					return envelope, err
				}
			} else if err := skipJSONDecoderValue(decoder); err != nil {
				return envelope, err
			}
		case "stream":
			if err := decoder.Decode(&envelope.req.Stream); err != nil {
				return envelope, err
			}
		case "requestheaders":
			if requestHeadersSeen {
				if err := skipJSONDecoderValue(decoder); err != nil {
					return envelope, err
				}
				continue
			}
			if err := decoder.Decode(&envelope.req.RequestHeaders); err != nil {
				return envelope, err
			}
			requestHeadersSeen = true
		case "responseheaders":
			if responseHeadersSeen {
				if err := skipJSONDecoderValue(decoder); err != nil {
					return envelope, err
				}
				continue
			}
			if err := decoder.Decode(&envelope.req.ResponseHeaders); err != nil {
				return envelope, err
			}
			responseHeadersSeen = true
		case "requestbody":
			if requestSeen {
				if err := skipJSONDecoderValue(decoder); err != nil {
					return envelope, err
				}
				continue
			}
			var raw []byte
			if err := decoder.Decode(&raw); err != nil {
				return envelope, err
			}
			requestFields = responseRequestStringFields(raw)
			requestSeen = true
		case "originalrequest":
			if originalSeen {
				if err := skipJSONDecoderValue(decoder); err != nil {
					return envelope, err
				}
				continue
			}
			var raw []byte
			if err := decoder.Decode(&raw); err != nil {
				return envelope, err
			}
			originalFields = responseRequestStringFields(raw)
			originalSeen = true
		case "body":
			if bodySeen {
				if err := skipJSONDecoderValue(decoder); err != nil {
					return envelope, err
				}
				continue
			}
			bodySeen = true
			body, hasMarker, err := decodeResponseInterceptBody(decoder)
			if err != nil {
				return envelope, err
			}
			envelope.bodyHadUsage = hasMarker
			if hasMarker && !envelope.req.Stream {
				envelope.decoded, envelope.usageFound = decodeUsagePayload(body, usageDecodeComplete)
			}
		case "statuscode":
			if err := decoder.Decode(&envelope.req.StatusCode); err != nil {
				return envelope, err
			}
		case "metadata":
			if metadataSeen {
				if err := skipJSONDecoderValue(decoder); err != nil {
					return envelope, err
				}
				continue
			}
			envelope.req.Metadata, err = decodeCompactResponseMetadata(decoder)
			if err != nil {
				return envelope, err
			}
			metadataSeen = true
		default:
			if err := skipJSONDecoderValue(decoder); err != nil {
				return envelope, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return envelope, err
	}
	if envelope.req.Model == "" {
		envelope.req.Model = firstNonEmpty(requestFields.model, originalFields.model)
	}
	if envelope.req.RequestedModel == "" {
		envelope.req.RequestedModel = firstNonEmpty(
			requestFields.requestedModel,
			originalFields.requestedModel,
			envelope.req.Model,
		)
	}
	if metadataString(envelope.req.Metadata, "service_tier") == "" {
		serviceTier := firstNonEmpty(requestFields.serviceTier, originalFields.serviceTier)
		if serviceTier != "" {
			if envelope.req.Metadata == nil {
				envelope.req.Metadata = make(map[string]any, 1)
			}
			envelope.req.Metadata["service_tier"] = serviceTier
		}
	}
	return envelope, nil
}

func decodeResponseInterceptBody(decoder *json.Decoder) ([]byte, bool, error) {
	var encoded string
	if err := decoder.Decode(&encoded); err != nil {
		return nil, false, err
	}
	if encoded == "" {
		return nil, false, nil
	}
	scratch := make([]byte, base64.StdEncoding.DecodedLen(32<<10))
	hasMarker, err := base64ChunkContainsUsage(encoded, scratch)
	if err != nil || !hasMarker {
		return nil, hasMarker, err
	}
	body, err := base64.StdEncoding.DecodeString(encoded)
	return body, true, err
}

func decodeCompactResponseMetadata(decoder *json.Decoder) (map[string]any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		if delim == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("response metadata is not a JSON object")
	}
	metadata := make(map[string]any)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("response metadata field name is not a string")
		}
		canonical, keep := compactResponseMetadataKey(key)
		if !keep {
			if err := skipJSONDecoderValue(decoder); err != nil {
				return nil, err
			}
			continue
		}
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if _, exists := metadata[canonical]; !exists {
			metadata[canonical] = value
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	return metadata, nil
}

func compactResponseMetadataKey(key string) (string, bool) {
	normalized := normalizeUsageFieldName(key)
	switch normalized {
	case "selectedauthid":
		return "selected_auth_id", true
	case "pinnedauthid":
		return "pinned_auth_id", true
	case "upstreamprovider":
		return "upstream_provider", true
	case "provider":
		return "provider", true
	case "selectedprovider":
		return "selected_provider", true
	case "requestedmodel":
		return "requested_model", true
	case "servicetier":
		return "service_tier", true
	case "reasoningeffort":
		return "reasoning_effort", true
	case "upstreambaseurl":
		return "upstream_base_url", true
	case "providerbaseurl":
		return "provider_base_url", true
	case "baseurl":
		return "base_url", true
	case "upstreamsource":
		return "upstream_source", true
	case "providersource":
		return "provider_source", true
	case "selectedsource":
		return "selected_source", true
	case "requestpath":
		return "request_path", true
	case "endpoint":
		return "endpoint", true
	case "requestendpoint":
		return "request_endpoint", true
	case "path":
		return "path", true
	case "uri":
		return "uri", true
	case "url":
		return "url", true
	case "route":
		return "route", true
	case "authindex":
		return "auth_index", true
	case "selectedauthindex":
		return "selected_auth_index", true
	case "pinnedauthindex":
		return "pinned_auth_index", true
	case "authtype":
		return "auth_type", true
	case "selectedauthtype":
		return "selected_auth_type", true
	case "pinnedauthtype":
		return "pinned_auth_type", true
	case "responseid":
		return "response_id", true
	case "requestid":
		return "request_id", true
	case "streamid":
		return "stream_id", true
	default:
		return "", false
	}
}

func responseInterceptEnvelopeBytes(envelope responseInterceptEnvelope) int {
	bytes := 256 + len(envelope.req.SourceFormat) + len(envelope.req.Model) +
		len(envelope.req.RequestedModel) + len(envelope.req.correlationID)
	bytes += usageDetailRetainedBytes(envelope.decoded.detail)
	bytes += responseHeadersRetainedBytes(envelope.req.RequestHeaders)
	bytes += responseHeadersRetainedBytes(envelope.req.ResponseHeaders)
	for key, value := range envelope.req.Metadata {
		bytes += len(key) + len(metadataValueString(value)) + 32
	}
	if bytes < 256 {
		return 256
	}
	return bytes
}

func usageDetailRetainedBytes(detail UsageDetail) int {
	return 8 * 7
}

func responseHeadersRetainedBytes(headers map[string][]string) int {
	retained := 0
	for key, values := range headers {
		retained += len(key) + 32
		for _, value := range values {
			retained += len(value)
		}
	}
	return retained
}

func handleResponseStreamChunk(requestBody []byte) ([]byte, error) {
	statistics := stats
	fallbacks := usageFallbacks
	if !responseStreamChunkMayContainUsage(requestBody) {
		return okEnvelopeJSON("{}")
	}
	// Parse while the callback buffer is available, then hand only the compact
	// usage record to the asynchronous processor. Capturing requestBody here
	// would retain the complete cumulative HistoryChunks payload until the
	// queue task runs.
	req, hasUsage, err := decodeResponseStreamChunkForUsage(requestBody)
	if err != nil || !hasUsage {
		return okEnvelopeJSON("{}")
	}
	return handleResponseStreamChunkRequest(statistics, fallbacks, req)
}

func handleResponseStreamChunkRequest(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, req ResponseStreamChunkRequest) ([]byte, error) {
	return handleResponseStreamChunkRequestAndTrack(statistics, fallbacks, req, false, false)
}

func handleResponseStreamChunkRequestWithTerminal(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, req ResponseStreamChunkRequest, terminal bool) ([]byte, error) {
	return handleResponseStreamChunkRequestAndTrack(statistics, fallbacks, req, terminal, true)
}

func handleResponseStreamChunkRequestAndTrack(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, req ResponseStreamChunkRequest, terminal bool, track bool) ([]byte, error) {
	record, ok := usageRecordFromResponseStreamChunk(req)
	if !ok {
		if track && terminal && streamUsages != nil {
			record = UsageRecord{correlationID: firstNonEmpty(
				req.correlationID,
				metadataString(req.Metadata, "response_id", "responseId", "stream_id", "streamId", "request_id", "requestId"),
			)}
		} else {
			if responseStatusIsSuccessful(req.StatusCode) && responseStreamRequestMayContainUsage(req) && statistics != nil {
				statistics.recordUsageParseFailure()
			}
			return okEnvelopeJSON("{}")
		}
	} else {
		sanitizeUsageRecordForStats(statistics, &record)
	}
	var superseded []usageFallbackSupersession
	if usageDetailHasTokens(record.Detail) && (!track || strings.TrimSpace(record.correlationID) == "") {
		superseded = supersededStreamUsageFingerprints(req)
	}
	compactBytes := usageFallbackRecordBytes(record) + len(superseded)*64
	chunkIndex := req.ChunkIndex
	task := func() {
		processResponseStreamUsageRecord(statistics, fallbacks, record, chunkIndex, terminal, track, superseded)
	}
	switch deferUsageCallback(statistics, task, compactBytes) {
	case usageCallbackQueued, usageCallbackDropped:
		return okEnvelopeJSON("{}")
	}
	processResponseStreamUsageRecord(statistics, fallbacks, record, chunkIndex, terminal, track, superseded)
	return okEnvelopeJSON("{}")
}

func pluginCallNeedsRequestCopy(method string, requestBody []byte) bool {
	if method != "response.intercept_stream_chunk" {
		return true
	}
	return responseStreamChunkMayContainUsage(requestBody)
}

func responseStreamChunkMayContainUsage(requestBody []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(requestBody))
	token, err := decoder.Token()
	if err != nil {
		return true
	}
	objectStart, ok := token.(json.Delim)
	if !ok || objectStart != '{' {
		return true
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return true
		}
		key, ok := keyToken.(string)
		if !ok {
			return true
		}
		if strings.EqualFold(key, "body") {
			var body []byte
			if err := decoder.Decode(&body); err != nil {
				return true
			}
			if len(bytes.TrimSpace(body)) == 0 {
				continue
			}
			if responseBodyMayContainUsage(body) {
				return true
			}
			continue
		}
		if strings.EqualFold(key, "historychunks") {
			hasUsage, err := decodeStreamHistoryUsageChunks(decoder, nil)
			if err != nil {
				return true
			}
			if hasUsage {
				return true
			}
			continue
		}
		if err := skipJSONDecoderValue(decoder); err != nil {
			return true
		}
	}
	return false
}

func responseBodyMayContainUsage(body []byte) bool {
	for _, marker := range responseUsageMarkers {
		if bytes.Contains(body, marker) {
			return true
		}
	}
	return false
}

var responseUsageMarkers = [...][]byte{
	[]byte(`"usage"`),
	[]byte(`"total_usage"`),
	[]byte(`"usageMetadata"`),
	[]byte(`"usage_metadata"`),
}

const (
	maxStreamHistoryUsageChunks = 32
	maxStreamHistoryUsageBytes  = 1 << 20
)

// decodeResponseStreamChunkForUsage parses a host-owned stream callback
// without first copying the complete JSON payload. HistoryChunks can contain
// the entire response transcript on every callback; only a bounded tail of
// chunks that contain usage markers is retained for fallback supersession.
func decodeResponseStreamChunkForUsage(data []byte) (ResponseStreamChunkRequest, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var req ResponseStreamChunkRequest
	req.Stream = true
	if token, err := decoder.Token(); err != nil {
		return req, false, err
	} else if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return req, false, fmt.Errorf("response stream chunk is not a JSON object")
	}
	bodySet := false
	currentHasUsage := false
	requestServiceTier := ""
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return req, false, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return req, false, fmt.Errorf("response stream chunk field name is not a string")
		}
		normalized := normalizeResponseStreamFieldName(key)
		switch normalized {
		case "sourceformat":
			if err := decoder.Decode(&req.SourceFormat); err != nil {
				return req, false, err
			}
		case "model":
			if err := decoder.Decode(&req.Model); err != nil {
				return req, false, err
			}
		case "requestedmodel":
			if err := decoder.Decode(&req.RequestedModel); err != nil {
				return req, false, err
			}
		case "stream":
			var stream bool
			if err := decoder.Decode(&stream); err != nil {
				return req, false, err
			}
			if stream {
				req.Stream = true
			}
		case "requestheaders":
			if err := decoder.Decode(&req.RequestHeaders); err != nil {
				return req, false, err
			}
		case "responseheaders":
			if err := decoder.Decode(&req.ResponseHeaders); err != nil {
				return req, false, err
			}
		case "responseid", "requestid", "streamid":
			var correlationID string
			if err := decoder.Decode(&correlationID); err != nil {
				return req, false, err
			}
			if req.correlationID == "" {
				req.correlationID = correlationID
			}
		case "originalrequest", "requestbody":
			// The request payload is only a source for three small metadata
			// fields. Decode it into a local temporary and do not retain the
			// potentially multi-megabyte body in the queued request.
			var raw []byte
			if err := decoder.Decode(&raw); err != nil {
				return req, false, err
			}
			fields := responseRequestStringFields(raw)
			if req.Model == "" {
				req.Model = fields.model
			}
			if req.RequestedModel == "" {
				req.RequestedModel = fields.requestedModel
			}
			requestServiceTier = firstNonEmpty(requestServiceTier, fields.serviceTier)
		case "body":
			var body []byte
			if err := decoder.Decode(&body); err != nil {
				return req, false, err
			}
			if !bodySet {
				req.Body = body
				bodySet = true
				currentHasUsage = responseBodyMayContainUsage(body)
			}
		case "statuscode":
			if err := decoder.Decode(&req.StatusCode); err != nil {
				return req, false, err
			}
		case "metadata":
			if err := decoder.Decode(&req.Metadata); err != nil {
				return req, false, err
			}
		case "historychunks":
			historyHasUsage, err := decodeStreamHistoryUsageChunks(decoder, &req.HistoryChunks)
			if err != nil {
				return req, false, err
			}
			currentHasUsage = currentHasUsage || historyHasUsage
		case "chunkindex":
			if err := decoder.Decode(&req.ChunkIndex); err != nil {
				return req, false, err
			}
		default:
			if err := skipJSONDecoderValue(decoder); err != nil {
				return req, false, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return req, false, err
	}
	if requestServiceTier != "" && metadataString(req.Metadata, "service_tier") == "" {
		if req.Metadata == nil {
			req.Metadata = make(map[string]any)
		}
		req.Metadata["service_tier"] = requestServiceTier
	}
	return req, currentHasUsage, nil
}

func normalizeResponseStreamFieldName(key string) string {
	switch key {
	case "SourceFormat", "source_format", "sourceFormat":
		return "sourceformat"
	case "Model", "model":
		return "model"
	case "RequestedModel", "requested_model", "requestedModel":
		return "requestedmodel"
	case "ResponseID", "response_id", "responseId":
		return "responseid"
	case "RequestID", "request_id", "requestId":
		return "requestid"
	case "StreamID", "stream_id", "streamId":
		return "streamid"
	case "Stream", "stream":
		return "stream"
	case "RequestHeaders", "request_headers", "requestHeaders":
		return "requestheaders"
	case "ResponseHeaders", "response_headers", "responseHeaders":
		return "responseheaders"
	case "OriginalRequest", "original_request", "originalRequest":
		return "originalrequest"
	case "RequestBody", "request_body", "requestBody":
		return "requestbody"
	case "Body", "body":
		return "body"
	case "StatusCode", "status_code", "statusCode":
		return "statuscode"
	case "Metadata", "metadata":
		return "metadata"
	case "HistoryChunks", "history_chunks", "historyChunks":
		return "historychunks"
	case "ChunkIndex", "chunk_index", "chunkIndex":
		return "chunkindex"
	default:
		return strings.ToLower(strings.ReplaceAll(key, "_", ""))
	}
}

func decodeStreamHistoryUsageChunks(decoder *json.Decoder, target *[][]byte) (bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return false, err
	}
	if token == nil {
		return false, nil
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return false, fmt.Errorf("HistoryChunks is not an array")
	}
	hasUsage := false
	var retainedBytes int
	const encodedBlockBytes = 32 << 10
	scratch := make([]byte, base64.StdEncoding.DecodedLen(encodedBlockBytes))
	for decoder.More() {
		var encoded string
		if err := decoder.Decode(&encoded); err != nil {
			return false, err
		}
		chunkHasUsage, err := base64ChunkContainsUsage(encoded, scratch)
		if err != nil {
			return false, err
		}
		if !chunkHasUsage {
			continue
		}
		hasUsage = true
		if target == nil {
			continue
		}
		chunk, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return false, err
		}
		*target = append(*target, chunk)
		retainedBytes += len(chunk)
		for len(*target) > maxStreamHistoryUsageChunks || retainedBytes > maxStreamHistoryUsageBytes {
			if len(*target) == 0 {
				break
			}
			retainedBytes -= len((*target)[0])
			*target = (*target)[1:]
		}
	}
	_, err = decoder.Token()
	return hasUsage, err
}

// base64ChunkContainsUsage avoids materializing every HistoryChunks payload
// when a stream callback carries the full history. The JSON representation of
// []byte is Base64, so decoding into a small scratch buffer lets the common
// non-usage path avoid both a large allocation and a second full scan. A full
// decode is only performed for chunks that actually contain a usage marker.
func base64ChunkContainsUsage(encoded string, scratch []byte) (bool, error) {
	if encoded == "" {
		return false, nil
	}
	const encodedBlockBytes = 32 << 10
	const carryBytes = 32
	if len(scratch) < base64.StdEncoding.DecodedLen(encodedBlockBytes) {
		scratch = make([]byte, base64.StdEncoding.DecodedLen(encodedBlockBytes))
	}
	var carry [carryBytes]byte
	carryLen := 0
	for offset := 0; offset < len(encoded); {
		end := offset + encodedBlockBytes
		if end > len(encoded) {
			end = len(encoded)
		}
		block := []byte(encoded[offset:end])
		n, err := base64.StdEncoding.Decode(scratch, block)
		if err != nil {
			return false, err
		}
		if usageBytesContainMarker(carry[:carryLen], scratch[:n]) {
			return true, nil
		}
		if n >= carryBytes {
			copy(carry[:], scratch[n-carryBytes:n])
			carryLen = carryBytes
		} else {
			keep := carryLen + n
			if keep > carryBytes {
				shift := keep - carryBytes
				copy(carry[:], carry[shift:carryLen])
				carryLen = carryBytes - n
			}
			copy(carry[carryLen:], scratch[:n])
			carryLen += n
		}
		offset = end
	}
	return false, nil
}

func usageBytesContainMarker(carry, block []byte) bool {
	if len(carry) == 0 {
		for _, marker := range responseUsageMarkers {
			if bytes.Contains(block, marker) {
				return true
			}
		}
		return false
	}
	var combined [64]byte
	length := copy(combined[:], carry)
	length += copy(combined[length:], block)
	for _, marker := range responseUsageMarkers {
		if bytes.Contains(block, marker) || bytes.Contains(combined[:length], marker) {
			return true
		}
	}
	return false
}

func skipJSONDecoderValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return err
			}
			if err := skipJSONDecoderValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := skipJSONDecoderValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	_, err = decoder.Token()
	return err
}

func processResponseIntercept(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, requestBody []byte) {
	envelope, err := inspectResponseInterceptEnvelope(requestBody)
	if err != nil {
		return
	}
	processResponseInterceptEnvelope(statistics, fallbacks, envelope)
}

func processResponseInterceptEnvelope(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, envelope responseInterceptEnvelope) {
	if envelope.req.Stream || !responseStatusIsSuccessful(envelope.req.StatusCode) {
		return
	}
	if envelope.usageFound {
		record, ok := usageRecordFromDecodedUsage(envelope.req, envelope.decoded)
		if !ok {
			if envelope.bodyHadUsage && statistics != nil {
				statistics.recordUsageParseFailure()
			}
			return
		}
		record.usageOrigin = usageOriginInterceptBody
		sanitizeUsageRecordForStats(statistics, &record)
		if fallbacks != nil {
			fallbacks.ScheduleForStats(statistics, record)
		}
		return
	}
	if envelope.bodyHadUsage && statistics != nil {
		statistics.recordUsageParseFailure()
	}
}

func processResponseStreamChunk(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, requestBody []byte) {
	req, hasUsage, err := decodeResponseStreamChunkForUsage(requestBody)
	if err != nil || !hasUsage {
		return
	}
	processResponseStreamChunkRequest(statistics, fallbacks, req)
}

func processResponseStreamChunkRequest(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, req ResponseStreamChunkRequest) {
	processResponseStreamChunkRequestWithTerminalAndTrack(statistics, fallbacks, req, false, false)
}

func processResponseStreamChunkRequestWithTerminal(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, req ResponseStreamChunkRequest, terminal bool) {
	processResponseStreamChunkRequestWithTerminalAndTrack(statistics, fallbacks, req, terminal, true)
}

func processResponseStreamChunkRequestWithTerminalAndTrack(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, req ResponseStreamChunkRequest, terminal bool, track bool) {
	if record, ok := usageRecordFromResponseStreamChunk(req); ok {
		sanitizeUsageRecordForStats(statistics, &record)
		if track && terminal && streamUsages != nil {
			streamUsages.Observe(statistics, fallbacks, record, req.ChunkIndex, true)
			return
		}
		if track && !terminal && strings.TrimSpace(record.correlationID) != "" && streamUsages != nil {
			streamUsages.Observe(statistics, fallbacks, record, req.ChunkIndex, false)
			return
		}
		if fallbacks != nil {
			fallbacks.Supersede(supersededStreamUsageFingerprints(req))
			fallbacks.ScheduleForStats(statistics, record)
		}
		return
	} else if track && terminal && streamUsages != nil {
		streamUsages.Observe(statistics, fallbacks, UsageRecord{correlationID: firstNonEmpty(req.correlationID, metadataString(req.Metadata, "response_id", "responseId", "stream_id", "streamId", "request_id", "requestId"))}, req.ChunkIndex, true)
		return
	}
	if responseStatusIsSuccessful(req.StatusCode) && responseStreamRequestMayContainUsage(req) {
		statistics.recordUsageParseFailure()
	}
}

func processResponseStreamUsageRecord(statistics *RequestStatistics, fallbacks *usageFallbackCoordinator, record UsageRecord, chunkIndex int, terminal bool, track bool, superseded []usageFallbackSupersession) {
	if track && terminal && streamUsages != nil {
		streamUsages.Observe(statistics, fallbacks, record, chunkIndex, true)
		return
	}
	if track && !terminal && strings.TrimSpace(record.correlationID) != "" && streamUsages != nil {
		streamUsages.Observe(statistics, fallbacks, record, chunkIndex, false)
		return
	}
	if fallbacks != nil {
		fallbacks.Supersede(superseded)
		if usageDetailHasTokens(record.Detail) {
			fallbacks.ScheduleForStats(statistics, record)
		}
	}
}

func sanitizeUsageRecordForStats(statistics *RequestStatistics, record *UsageRecord) {
	if statistics == nil || record == nil || len(record.ResponseHeaders) == 0 {
		return
	}
	statistics.mu.RLock()
	whitelist := statistics.logResponseHeaders
	statistics.mu.RUnlock()
	record.ResponseHeaders = filterHeaders(record.ResponseHeaders, whitelist)
}

func responseStatusIsSuccessful(statusCode int) bool {
	return statusCode == 0 || statusCode >= 200 && statusCode < 300
}

func responseStreamRequestMayContainUsage(req ResponseStreamChunkRequest) bool {
	if responseBodyMayContainUsage(req.Body) && !responseStreamPayloadIsIgnored(req.Body) {
		return true
	}
	for _, chunk := range req.HistoryChunks {
		if responseBodyMayContainUsage(chunk) && !responseStreamPayloadIsIgnored(chunk) {
			return true
		}
	}
	return false
}

func responseStreamPayloadIsIgnored(payload []byte) bool {
	lower := bytes.ToLower(payload)
	return bytes.Contains(lower, []byte(`"message_start"`)) ||
		bytes.Contains(lower, []byte(`"message-start"`))
}

// supersededStreamUsageFingerprints returns dedup fingerprints for usage
// payloads carried by earlier chunks of the same stream. A later usage chunk
// (e.g. providers that attach running totals to every chunk, or Codex emitting
// usage on multiple response events) supersedes those pending fallbacks so
// only the most recent usage snapshot of the stream is committed.
type usageFallbackSupersession struct {
	key           string
	correlationID string
}

func supersededStreamUsageFingerprints(req ResponseStreamChunkRequest) []usageFallbackSupersession {
	if len(req.HistoryChunks) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, 2)
	keys := make([]usageFallbackSupersession, 0, 2)
	for _, chunk := range req.HistoryChunks {
		if len(bytes.TrimSpace(chunk)) == 0 {
			continue
		}
		record, ok := usageRecordFromStreamPayload(req.ResponseInterceptRequest, chunk)
		if !ok {
			continue
		}
		key := usageFallbackMatchKey(record)
		if key == "" {
			continue
		}
		identity := key + "\x00" + strings.TrimSpace(record.correlationID)
		if _, dup := seen[identity]; dup {
			continue
		}
		seen[identity] = struct{}{}
		keys = append(keys, usageFallbackSupersession{key: key, correlationID: strings.TrimSpace(record.correlationID)})
	}
	return keys
}

func usageRecordFromResponseIntercept(req ResponseInterceptRequest) (UsageRecord, bool) {
	if req.Stream || req.StatusCode < 200 || req.StatusCode >= 300 || len(bytes.TrimSpace(req.Body)) == 0 {
		return UsageRecord{}, false
	}
	responseValues := responseJSONValues(req.Body)
	return usageRecordFromResponseValues(req, responseValues)
}

func usageRecordFromResponseStreamChunk(req ResponseStreamChunkRequest) (UsageRecord, bool) {
	if req.StatusCode != 0 && (req.StatusCode < 200 || req.StatusCode >= 300) {
		return UsageRecord{}, false
	}
	if len(bytes.TrimSpace(req.Body)) > 0 {
		if record, ok := usageRecordFromStreamPayload(req.ResponseInterceptRequest, req.Body); ok {
			record.usageOrigin = usageOriginInterceptBody
			return record, true
		}
	}
	for index := len(req.HistoryChunks) - 1; index >= 0; index-- {
		chunk := req.HistoryChunks[index]
		if len(bytes.TrimSpace(chunk)) == 0 {
			continue
		}
		if record, ok := usageRecordFromStreamPayload(req.ResponseInterceptRequest, chunk); ok {
			record.usageOrigin = usageOriginInterceptHistory
			return record, true
		}
	}
	return UsageRecord{}, false
}

func usageRecordFromResponseValues(req ResponseInterceptRequest, responseValues []any) (UsageRecord, bool) {
	return usageRecordFromValues(req, responseValues, usageDetailPaths)
}

// usageRecordFromStreamValues mirrors usageRecordFromResponseValues but skips
// the message_start-style "message.usage" path: in Claude streams that node
func usageRecordFromStreamValues(req ResponseInterceptRequest, responseValues []any) (UsageRecord, bool) {
	return usageRecordFromValues(req, responseValues, usageDetailStreamPaths)
}

func usageRecordFromStreamPayload(req ResponseInterceptRequest, payload []byte) (UsageRecord, bool) {
	decoded, ok := decodeUsagePayload(payload, usageDecodeStream)
	if !ok {
		return UsageRecord{}, false
	}
	return usageRecordFromDecodedUsage(req, decoded)
}

func usageRecordFromValues(req ResponseInterceptRequest, responseValues []any, detailPaths []string) (UsageRecord, bool) {
	detail, ok := usageDetailFromResponseValues(responseValues, detailPaths)
	if !ok {
		return UsageRecord{}, false
	}
	model := firstNonEmpty(
		jsonStringPathFromValues(responseValues, "model", "response.model", "message.model"),
	)
	correlationID := responseCorrelationID(req, responseValues)
	return usageRecordFromDecodedUsage(req, decodedUsage{model: model, correlationID: correlationID, detail: detail})
}

func usageRecordFromDecodedUsage(req ResponseInterceptRequest, decoded decodedUsage) (UsageRecord, bool) {
	detail := decoded.detail
	if !usageDetailHasTokens(detail) {
		return UsageRecord{}, false
	}
	authID := firstNonEmpty(metadataString(req.Metadata, "selected_auth_id"), metadataString(req.Metadata, "pinned_auth_id"))
	// selected_auth_id is what CPA's conductor actually publishes and encodes
	// the upstream kind unambiguously; the plain provider metadata keys are
	// speculative and must not override it (a generic value like "claude"
	// would flip both grouping and cache accounting for compat upstreams).
	provider := firstNonEmpty(
		providerFromSelectedAuthID(req.Metadata),
		metadataString(req.Metadata, "upstream_provider", "provider", "selected_provider"),
		fallbackUsageProvider(req),
	)
	if responseUsesAnthropicUsageAccounting(req) {
		detail = normalizeAnthropicUsageDetail(detail, usageProviderFamily(provider))
	}
	model := firstNonEmpty(decoded.model, req.Model, req.RequestedModel)
	requestedModel := firstNonEmpty(req.RequestedModel, metadataString(req.Metadata, "requested_model"), model)
	serviceTier := metadataString(req.Metadata, "service_tier")
	if model == "" || requestedModel == "" || serviceTier == "" {
		fields := responseRequestStringFields(firstBytes(req.RequestBody, req.OriginalRequest))
		model = firstNonEmpty(model, fields.model)
		requestedModel = firstNonEmpty(requestedModel, fields.requestedModel, model)
		serviceTier = firstNonEmpty(serviceTier, fields.serviceTier)
	}
	model = firstNonEmpty(
		model,
		"unknown",
	)
	return UsageRecord{
		Provider:        provider,
		ExecutorType:    "ResponseInterceptorFallback",
		Model:           model,
		Alias:           requestedModel,
		APIKey:          apiKeyFromHeaders(req.RequestHeaders),
		AuthID:          authID,
		AuthIndex:       fallbackAuthIndex(req.Metadata, authID),
		AuthType:        fallbackAuthType(req.Metadata, authID),
		Endpoint:        responseInterceptEndpoint(req),
		ReasoningEffort: metadataString(req.Metadata, "reasoning_effort"),
		ServiceTier:     serviceTier,
		Stream:          req.Stream,
		RequestedAt:     time.Now(),
		Detail:          detail,
		BaseURL:         metadataString(req.Metadata, "upstream_base_url", "provider_base_url", "base_url", "baseURL"),
		Source:          metadataString(req.Metadata, "upstream_source", "provider_source", "selected_source"),
		ResponseHeaders: req.ResponseHeaders,
		correlationID:   firstNonEmpty(decoded.correlationID, responseCorrelationID(req, nil)),
	}, true
}

type responseRequestMetadata struct {
	model          string
	requestedModel string
	serviceTier    string
}

// responseRequestStringFields extracts the few request strings needed by the
// usage record without decoding a potentially large request body into
// map[string]any. It is used only when the callback envelope did not already
// provide the values.
func responseRequestStringFields(raw []byte) responseRequestMetadata {
	var fields responseRequestMetadata
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return fields
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return fields
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fields
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return fields
		}
		key, ok := keyToken.(string)
		if !ok {
			return fields
		}
		switch normalizeUsageFieldName(key) {
		case "model":
			value, err := decodeJSONScalarString(decoder)
			if err != nil {
				return fields
			}
			fields.model = firstNonEmpty(fields.model, value)
		case "requestedmodel":
			value, err := decodeJSONScalarString(decoder)
			if err != nil {
				return fields
			}
			fields.requestedModel = firstNonEmpty(fields.requestedModel, value)
		case "servicetier":
			value, err := decodeJSONScalarString(decoder)
			if err != nil {
				return fields
			}
			fields.serviceTier = firstNonEmpty(fields.serviceTier, value)
		default:
			if err := skipJSONDecoderValue(decoder); err != nil {
				return fields
			}
		}
		if fields.model != "" && fields.requestedModel != "" && fields.serviceTier != "" {
			break
		}
	}
	return fields
}

const maxUsageCorrelationIDLength = 256

func responseCorrelationID(req ResponseInterceptRequest, responseValues []any) string {
	value := firstNonEmpty(
		req.correlationID,
		jsonStringPathFromValues(responseValues, "response_id", "responseId", "response.id", "id"),
		metadataString(req.Metadata, "response_id", "responseId", "stream_id", "streamId", "request_id", "requestId"),
	)
	value = strings.TrimSpace(value)
	if len(value) > maxUsageCorrelationIDLength {
		return ""
	}
	return value
}

func responseInterceptEndpoint(req ResponseInterceptRequest) string {
	if endpoint := metadataString(req.Metadata, "request_path", "endpoint", "request_endpoint", "path", "uri", "url", "route"); endpoint != "" {
		return endpoint
	}
	switch strings.ToLower(strings.TrimSpace(req.SourceFormat)) {
	case "openai-response", "openai-responses":
		return "/v1/responses"
	default:
		return ""
	}
}

func responseUsesAnthropicUsageAccounting(req ResponseInterceptRequest) bool {
	value := strings.ToLower(strings.TrimSpace(req.SourceFormat))
	return strings.Contains(value, "anthropic") || strings.Contains(value, "claude")
}

// normalizeAnthropicUsageDetail aligns a Claude-shaped usage payload
// (input_tokens excludes cache reads/creations) with the accounting of the
// native usage record CPA produces for the same request, so the dedup
// fingerprints line up:
//   - Claude-family upstreams keep the exclusive input and count cache into
//     the total, mirroring CPA's native Claude usage parser.
//   - Every other upstream (openai-compatible, codex, ...) reports
//     prompt_tokens with cache included, so cache is folded into input.
func normalizeAnthropicUsageDetail(detail UsageDetail, providerFamily string) UsageDetail {
	cacheInput := detail.CacheReadTokens + detail.CacheCreationTokens
	if cacheInput <= 0 {
		return detail
	}
	if providerFamily == "claude" {
		expanded := detail.InputTokens + detail.OutputTokens + cacheInput
		if detail.TotalTokens < expanded {
			detail.TotalTokens = expanded
		}
		return detail
	}
	detail.InputTokens += cacheInput
	if detail.TotalTokens != 0 && detail.TotalTokens < detail.InputTokens+detail.OutputTokens {
		detail.TotalTokens = detail.InputTokens + detail.OutputTokens
	}
	return detail
}

func responseJSONValues(body []byte) []any {
	if root, ok := decodeJSONValue(body); ok {
		return []any{root}
	}
	return decodeSSEJSONValues(body)
}

func decodeSSEJSONValues(body []byte) []any {
	lines := strings.Split(string(body), "\n")
	values := make([]any, 0)
	var data strings.Builder
	flush := func() {
		raw := strings.TrimSpace(data.String())
		data.Reset()
		if raw == "" || raw == "[DONE]" {
			return
		}
		if value, ok := decodeJSONValue([]byte(raw)); ok {
			values = append(values, value)
			return
		}
		for _, line := range strings.Split(raw, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || line == "[DONE]" {
				continue
			}
			if value, ok := decodeJSONValue([]byte(line)); ok {
				values = append(values, value)
			}
		}
	}
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		if data.Len() > 0 {
			data.WriteByte('\n')
		}
		data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
	}
	flush()
	return values
}

func usageDetailFromResponseValues(values []any, detailPaths []string) (UsageDetail, bool) {
	var best UsageDetail
	var found bool
	for _, value := range values {
		detail, ok := usageDetailFromResponseRoot(value, detailPaths)
		if !ok {
			continue
		}
		if !found || usageDetailCompleteness(detail) >= usageDetailCompleteness(best) {
			best = detail
			found = true
		}
	}
	return best, found
}

func usageDetailCompleteness(detail UsageDetail) int64 {
	return absInt64(detail.TotalTokens) +
		absInt64(detail.InputTokens) +
		absInt64(detail.OutputTokens) +
		absInt64(detail.ReasoningTokens) +
		absInt64(detail.CachedTokens) +
		absInt64(detail.CacheReadTokens) +
		absInt64(detail.CacheCreationTokens)
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

// usageDetailPaths lists the JSON paths probed for usage payloads in complete
// (non-stream) response bodies.
var usageDetailPaths = []string{
	"usage",
	"response.usage",
	"message.usage",
	"total_usage",
	"metadata.usage",
	"metadata.total_usage",
	"usageMetadata",
	"usage_metadata",
	"response.usageMetadata",
	"response.usage_metadata",
}

// usageDetailStreamPaths is usageDetailPaths without "message.usage": in a
// Claude SSE stream that path only appears on message_start, whose usage is a
// pre-generation snapshot rather than the request's final usage.
var usageDetailStreamPaths = []string{
	"usage",
	"response.usage",
	"total_usage",
	"metadata.usage",
	"metadata.total_usage",
	"usageMetadata",
	"usage_metadata",
	"response.usageMetadata",
	"response.usage_metadata",
}

func usageDetailFromResponseRoot(root any, detailPaths []string) (UsageDetail, bool) {
	for _, path := range detailPaths {
		if node, ok := jsonValuePath(root, path); ok {
			if detail, ok := usageDetailFromValue(node); ok {
				return detail, true
			}
		}
	}
	return UsageDetail{}, false
}

func usageDetailFromValue(value any) (UsageDetail, bool) {
	raw, err := json.Marshal(value)
	if err != nil {
		return UsageDetail{}, false
	}
	var detail UsageDetail
	if err := json.Unmarshal(raw, &detail); err != nil {
		return UsageDetail{}, false
	}
	if m, ok := value.(map[string]any); ok {
		if detail.InputTokens == 0 {
			detail.InputTokens = firstJSONInt(m, "promptTokenCount", "inputTokenCount", "input_tokens", "prompt_tokens", "total_input_tokens")
		}
		if detail.OutputTokens == 0 {
			detail.OutputTokens = firstJSONInt(m, "candidatesTokenCount", "outputTokenCount", "output_tokens", "completion_tokens", "total_output_tokens")
		}
		if detail.ReasoningTokens == 0 {
			detail.ReasoningTokens = firstJSONInt(m, "thoughtsTokenCount", "reasoning_tokens", "total_thought_tokens")
		}
		if detail.ReasoningTokens == 0 {
			detail.ReasoningTokens = firstNestedJSONInt(m, "reasoning_tokens", "completion_tokens_details", "output_tokens_details")
		}
		if detail.CachedTokens == 0 {
			detail.CachedTokens = firstJSONInt(m, "cachedContentTokenCount", "cached_tokens", "total_cached_tokens")
		}
		if detail.CachedTokens == 0 {
			detail.CachedTokens = firstNestedJSONInt(m, "cached_tokens", "prompt_tokens_details", "input_tokens_details")
		}
		if detail.CacheReadTokens == 0 {
			detail.CacheReadTokens = firstJSONInt(m, "cache_read_tokens", "cacheReadTokens", "cache_read_input_tokens")
		}
		if detail.CacheCreationTokens == 0 {
			detail.CacheCreationTokens = firstJSONInt(m, "cache_creation_tokens", "cacheCreationTokens", "cache_creation_input_tokens")
		}
		if detail.CacheCreationTokens == 0 {
			detail.CacheCreationTokens = firstNestedJSONInt(m, "cache_write_tokens", "prompt_tokens_details", "input_tokens_details")
		}
		if detail.CacheCreationTokens == 0 {
			detail.CacheCreationTokens = firstNestedJSONInt(m, "cache_creation_tokens", "prompt_tokens_details", "input_tokens_details")
		}
		if detail.TotalTokens == 0 {
			detail.TotalTokens = firstJSONInt(m, "totalTokenCount", "total_tokens")
		}
	}
	if detail.TotalTokens == 0 {
		detail.TotalTokens = detail.InputTokens + detail.OutputTokens
	}
	if !usageDetailHasTokens(detail) {
		return UsageDetail{}, false
	}
	return detail, true
}

func usageDetailHasTokens(detail UsageDetail) bool {
	return detail.InputTokens != 0 ||
		detail.OutputTokens != 0 ||
		detail.ReasoningTokens != 0 ||
		detail.CachedTokens != 0 ||
		detail.CacheReadTokens != 0 ||
		detail.CacheCreationTokens != 0 ||
		detail.TotalTokens != 0
}

type usageFallbackCoordinator struct {
	mu                  sync.Mutex
	pending             map[string][]*pendingUsageFallback
	pendingByStream     map[string]*pendingUsageFallback
	nativeRecent        map[string][]usageFallbackOccurrence
	fallbackRecent      map[string][]usageFallbackOccurrence
	deadlines           usageFallbackDeadlineHeap
	wake                chan struct{}
	stop                chan struct{}
	done                chan struct{}
	sequence            uint64
	pendingCount        int
	nativeRecentCount   int
	fallbackRecentCount int
	pendingBytes        int
	nativeRecentBytes   int
	fallbackRecentBytes int
	lastCleanup         time.Time
	closed              bool
}

type pendingUsageFallback struct {
	key       string
	streamKey string
	record    UsageRecord
	requestAt time.Time
	deadline  time.Time
	sequence  uint64
	heapIndex int
	cancelled bool
	stats     *RequestStatistics
	bytes     int
}

type usageFallbackDeadlineHeap []*pendingUsageFallback

func (h usageFallbackDeadlineHeap) Len() int {
	return len(h)
}

func (h usageFallbackDeadlineHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].deadline.Before(h[j].deadline)
}

func (h usageFallbackDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *usageFallbackDeadlineHeap) Push(value any) {
	item := value.(*pendingUsageFallback)
	item.heapIndex = len(*h)
	*h = append(*h, item)
}

func (h *usageFallbackDeadlineHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.heapIndex = -1
	*h = old[:last]
	return item
}

type usageFallbackOccurrence struct {
	requestAt  time.Time
	observedAt time.Time
	record     UsageRecord
	stats      *RequestStatistics
	bytes      int
}

func newUsageFallbackCoordinator() *usageFallbackCoordinator {
	coordinator := &usageFallbackCoordinator{
		pending:         make(map[string][]*pendingUsageFallback),
		pendingByStream: make(map[string]*pendingUsageFallback),
		nativeRecent:    make(map[string][]usageFallbackOccurrence),
		fallbackRecent:  make(map[string][]usageFallbackOccurrence),
		wake:            make(chan struct{}, 1),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
	}
	heap.Init(&coordinator.deadlines)
	go coordinator.runScheduler()
	return coordinator
}

func (c *usageFallbackCoordinator) signalScheduler() {
	if c == nil || c.wake == nil {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *usageFallbackCoordinator) runScheduler() {
	if c == nil {
		return
	}
	defer close(c.done)
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		now := time.Now()
		c.cleanupMaybeLocked(now)
		if len(c.deadlines) == 0 {
			c.mu.Unlock()
			select {
			case <-c.wake:
				continue
			case <-c.stop:
				return
			}
		}
		wait := time.Until(c.deadlines[0].deadline)
		c.mu.Unlock()
		if wait <= 0 {
			c.commitDue()
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			c.commitDue()
		case <-c.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-c.stop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (c *usageFallbackCoordinator) commitDue() {
	if c == nil {
		return
	}
	due := make([]*pendingUsageFallback, 0)
	c.mu.Lock()
	if !c.closed {
		now := time.Now()
		for len(c.deadlines) > 0 {
			item := c.deadlines[0]
			if item == nil || item.deadline.After(now) {
				break
			}
			due = append(due, heap.Pop(&c.deadlines).(*pendingUsageFallback))
		}
	}
	c.mu.Unlock()
	for _, item := range due {
		c.commit(item)
	}
}

func (c *usageFallbackCoordinator) Schedule(record UsageRecord) {
	c.scheduleForStats(nil, record)
}

func (c *usageFallbackCoordinator) ScheduleForStats(statistics *RequestStatistics, record UsageRecord) {
	c.scheduleForStats(statistics, record)
}

func (c *usageFallbackCoordinator) scheduleForStats(statistics *RequestStatistics, record UsageRecord) {
	if c == nil {
		return
	}
	key := usageFallbackMatchKey(record)
	if key == "" {
		return
	}
	requestAt := record.RequestedAt
	if requestAt.IsZero() {
		requestAt = time.Now()
		record.RequestedAt = requestAt
	}
	delay := usageFallbackRecordDelay
	if delay < 0 {
		delay = 0
	}
	now := time.Now()
	recordBytes := usageFallbackRecordBytes(record)
	pending := &pendingUsageFallback{
		key:       key,
		streamKey: usageFallbackStreamKey(record),
		record:    record,
		requestAt: requestAt,
		deadline:  now.Add(delay),
		heapIndex: -1,
		stats:     usageFallbackStats(statistics),
		bytes:     recordBytes,
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.cleanupMaybeLocked(now)
	if nativeRecord, nativeStats, ok := c.consumeNativeRecentLocked(key, record, requestAt, now); ok {
		c.mu.Unlock()
		if nativeStats == nil {
			nativeStats = pending.stats
		}
		if nativeStats != nil {
			nativeStats.EnrichRecordedUsage(nativeRecord, record)
		}
		return
	}
	if pending.streamKey != "" {
		if previous := c.pendingByStream[pending.streamKey]; previous != nil && !previous.cancelled {
			previous.cancelled = true
			c.removePendingLocked(previous)
		}
	}
	if c.pendingCount >= maxUsageFallbackPending {
		c.evictOldestPendingLocked()
	}
	for c.retainedBytesLocked()+recordBytes > maxUsageFallbackRetainedBytes {
		if c.pendingCount > 0 {
			c.evictOldestPendingLocked()
			continue
		}
		if c.nativeRecentCount > 0 {
			c.evictOldestNativeRecentLocked()
			continue
		}
		if c.fallbackRecentCount > 0 {
			c.evictOldestFallbackRecentLocked()
			continue
		}
		break
	}
	if c.retainedBytesLocked()+recordBytes > maxUsageFallbackRetainedBytes {
		c.mu.Unlock()
		if pending.stats != nil {
			pending.stats.recordUsageCallbackDrop()
		}
		return
	}
	c.pending[key] = append(c.pending[key], pending)
	if pending.streamKey != "" {
		c.pendingByStream[pending.streamKey] = pending
	}
	c.pendingCount++
	c.pendingBytes += recordBytes
	c.sequence++
	pending.sequence = c.sequence
	wasHead := len(c.deadlines) == 0 || pending.deadline.Before(c.deadlines[0].deadline)
	heap.Push(&c.deadlines, pending)
	c.mu.Unlock()
	if wasHead {
		c.signalScheduler()
	}
}

func (c *usageFallbackCoordinator) HandleNative(record UsageRecord) (UsageRecord, bool) {
	return c.HandleNativeForStats(nil, record)
}

func (c *usageFallbackCoordinator) HandleNativeForStats(statistics *RequestStatistics, record UsageRecord) (UsageRecord, bool) {
	if c == nil {
		return record, true
	}
	key := usageFallbackMatchKey(record)
	if key == "" {
		return record, true
	}
	requestAt := record.RequestedAt
	if requestAt.IsZero() {
		requestAt = time.Now()
		record.RequestedAt = requestAt
	}
	c.mu.Lock()
	now := time.Now()
	c.cleanupMaybeLocked(now)
	if pending := c.popPendingLocked(key, record); pending != nil {
		pending.cancelled = true
		c.mu.Unlock()
		c.signalScheduler()
		return enrichUsageRecord(record, pending.record), true
	}
	if fallbackRecord, fallbackStats, ok := c.matchesFallbackRecentLocked(key, record, requestAt, now); ok {
		c.mu.Unlock()
		// The fallback is already counted. Late native usage only contributes
		// metadata such as the endpoint and must not create a second event.
		if fallbackStats == nil {
			fallbackStats = usageFallbackStats(statistics)
		}
		if fallbackStats != nil {
			fallbackStats.EnrichRecordedUsage(fallbackRecord, record)
		}
		return record, false
	}
	if c.nativeRecentCount >= maxUsageFallbackRecent {
		c.evictOldestNativeRecentLocked()
	}
	for c.retainedBytesLocked()+usageFallbackRecordBytes(record) > maxUsageFallbackRetainedBytes {
		if c.pendingCount > 0 {
			c.evictOldestPendingLocked()
			continue
		}
		if c.nativeRecentCount > 0 {
			c.evictOldestNativeRecentLocked()
			continue
		}
		if c.fallbackRecentCount > 0 {
			c.evictOldestFallbackRecentLocked()
			continue
		}
		break
	}
	recordBytes := usageFallbackRecordBytes(record)
	if c.retainedBytesLocked()+recordBytes > maxUsageFallbackRetainedBytes {
		c.mu.Unlock()
		return record, true
	}
	c.nativeRecent[key] = append(c.nativeRecent[key], usageFallbackOccurrence{
		requestAt:  requestAt,
		observedAt: now,
		record:     record,
		stats:      usageFallbackStats(statistics),
		bytes:      recordBytes,
	})
	c.nativeRecentCount++
	c.nativeRecentBytes += recordBytes
	c.mu.Unlock()
	return record, true
}

func usageFallbackStats(statistics *RequestStatistics) *RequestStatistics {
	if statistics != nil {
		return statistics
	}
	return stats
}

func enrichUsageRecord(record UsageRecord, enrichment UsageRecord) UsageRecord {
	if strings.TrimSpace(record.APIKey) == "" {
		record.APIKey = strings.TrimSpace(enrichment.APIKey)
	}
	if strings.TrimSpace(record.Endpoint) == "" {
		record.Endpoint = strings.TrimSpace(enrichment.Endpoint)
	}
	if strings.TrimSpace(record.ReasoningEffort) == "" {
		record.ReasoningEffort = strings.TrimSpace(enrichment.ReasoningEffort)
	}
	if enrichment.Stream {
		record.Stream = true
	}
	return record
}

// Supersede cancels pending fallbacks whose fingerprints were derived from
// earlier usage-bearing chunks of the same stream; the caller schedules a
// fresher snapshot right after. Fallbacks already committed cannot be
// retracted — late native records still reconcile through fallbackRecent.
func (c *usageFallbackCoordinator) Supersede(entries []usageFallbackSupersession) {
	if c == nil || len(entries) == 0 {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	for _, entry := range entries {
		if entry.key == "" {
			continue
		}
		if pending := c.popPendingLocked(entry.key, UsageRecord{correlationID: entry.correlationID}); pending != nil {
			pending.cancelled = true
		}
	}
	c.mu.Unlock()
	c.signalScheduler()
}

func (c *usageFallbackCoordinator) Flush() {
	if c == nil {
		return
	}
	var records []*pendingUsageFallback
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.stop)
	}
	for key, pending := range c.pending {
		for _, item := range pending {
			if item == nil || item.cancelled {
				continue
			}
			item.cancelled = true
			c.removePendingHeapLocked(item)
			records = append(records, item)
		}
		delete(c.pending, key)
	}
	c.pendingCount = 0
	c.pendingBytes = 0
	c.mu.Unlock()
	if c.done != nil {
		<-c.done
	}
	for _, pending := range records {
		if pending == nil {
			continue
		}
		statistics := usageFallbackStats(pending.stats)
		if statistics != nil {
			statistics.Record(pending.record)
		}
	}
}

func (c *usageFallbackCoordinator) commit(pending *pendingUsageFallback) {
	if c == nil || pending == nil {
		return
	}
	c.mu.Lock()
	if c.closed || pending.cancelled {
		c.removePendingLocked(pending)
		c.mu.Unlock()
		return
	}
	pending.cancelled = true
	c.removePendingLocked(pending)
	now := time.Now()
	if c.fallbackRecentCount >= maxUsageFallbackRecent {
		c.evictOldestFallbackRecentLocked()
	}
	recordBytes := usageFallbackRecordBytes(pending.record)
	for c.retainedBytesLocked()+recordBytes > maxUsageFallbackRetainedBytes {
		switch {
		case c.pendingCount > 0:
			c.evictOldestPendingLocked()
		case c.nativeRecentCount > 0:
			c.evictOldestNativeRecentLocked()
		case c.fallbackRecentCount > 0:
			c.evictOldestFallbackRecentLocked()
		default:
			break
		}
		if c.pendingCount == 0 && c.nativeRecentCount == 0 && c.fallbackRecentCount == 0 {
			break
		}
	}
	c.fallbackRecent[pending.key] = append(c.fallbackRecent[pending.key], usageFallbackOccurrence{
		requestAt:  pending.requestAt,
		observedAt: now,
		record:     pending.record,
		stats:      pending.stats,
		bytes:      recordBytes,
	})
	c.fallbackRecentCount++
	c.fallbackRecentBytes += recordBytes
	c.cleanupMaybeLocked(now)
	record := pending.record
	c.mu.Unlock()
	// A native record for the same credential may have arrived while this
	// fallback was waiting; prefer the CPA auth index it taught us.
	if record.AuthID != "" && record.AuthIndex == safeCredentialIdentity(record.AuthID) {
		if learned := authIndexes.Lookup(record.AuthID); learned != "" {
			record.AuthIndex = learned
		}
	}
	statistics := usageFallbackStats(pending.stats)
	if statistics != nil {
		statistics.Record(record)
	}
}

func (c *usageFallbackCoordinator) popPendingLocked(key string, record UsageRecord) *pendingUsageFallback {
	items := c.pending[key]
	for _, item := range items {
		if item == nil || item.cancelled {
			continue
		}
		if !usageFallbackRecordsCompatible(item.record, record) {
			continue
		}
		c.removePendingLocked(item)
		return item
	}
	if len(items) == 0 {
		delete(c.pending, key)
	}
	return nil
}

func (c *usageFallbackCoordinator) removePendingLocked(pending *pendingUsageFallback) {
	if pending == nil {
		return
	}
	if pending.streamKey != "" && c.pendingByStream[pending.streamKey] == pending {
		delete(c.pendingByStream, pending.streamKey)
	}
	c.removePendingHeapLocked(pending)
	items := c.pending[pending.key]
	for i, item := range items {
		if item == pending {
			c.pending[pending.key] = append(items[:i], items[i+1:]...)
			if len(c.pending[pending.key]) == 0 {
				delete(c.pending, pending.key)
			}
			if c.pendingCount > 0 {
				c.pendingCount--
			}
			c.pendingBytes -= pending.bytes
			if c.pendingBytes < 0 {
				c.pendingBytes = 0
			}
			return
		}
	}
}

func (c *usageFallbackCoordinator) removePendingHeapLocked(pending *pendingUsageFallback) {
	if c == nil || pending == nil || pending.heapIndex < 0 || pending.heapIndex >= len(c.deadlines) {
		return
	}
	if c.deadlines[pending.heapIndex] != pending {
		pending.heapIndex = -1
		return
	}
	heap.Remove(&c.deadlines, pending.heapIndex)
}

func (c *usageFallbackCoordinator) consumeNativeRecentLocked(key string, record UsageRecord, requestAt time.Time, now time.Time) (UsageRecord, *RequestStatistics, bool) {
	items := c.nativeRecent[key]
	for i, item := range items {
		if now.Sub(item.observedAt) > usageFallbackNativeRecentWindow {
			continue
		}
		if !usageFallbackRecordsCompatible(item.record, record) ||
			!usageFallbackNativeRequestTimeCompatible(item.requestAt, requestAt) {
			continue
		}
		c.nativeRecent[key] = append(items[:i], items[i+1:]...)
		if len(c.nativeRecent[key]) == 0 {
			delete(c.nativeRecent, key)
		}
		if c.nativeRecentCount > 0 {
			c.nativeRecentCount--
		}
		c.nativeRecentBytes -= item.bytes
		if c.nativeRecentBytes < 0 {
			c.nativeRecentBytes = 0
		}
		return item.record, item.stats, true
	}
	return UsageRecord{}, nil, false
}

func (c *usageFallbackCoordinator) matchesFallbackRecentLocked(key string, record UsageRecord, requestAt time.Time, now time.Time) (UsageRecord, *RequestStatistics, bool) {
	items := c.fallbackRecent[key]
	for i, item := range items {
		if now.Sub(item.observedAt) > usageFallbackLateNativeWindow {
			continue
		}
		if usageFallbackRecordsCompatible(item.record, record) &&
			usageFallbackLateNativeRequestTimeCompatible(requestAt, item) {
			c.fallbackRecent[key] = append(items[:i], items[i+1:]...)
			if len(c.fallbackRecent[key]) == 0 {
				delete(c.fallbackRecent, key)
			}
			if c.fallbackRecentCount > 0 {
				c.fallbackRecentCount--
			}
			c.fallbackRecentBytes -= item.bytes
			if c.fallbackRecentBytes < 0 {
				c.fallbackRecentBytes = 0
			}
			return item.record, item.stats, true
		}
	}
	return UsageRecord{}, nil, false
}

func usageFallbackNativeRequestTimeCompatible(nativeAt, fallbackAt time.Time) bool {
	if nativeAt.IsZero() || fallbackAt.IsZero() {
		return true
	}
	// Native RequestedAt can represent the start of a long request, while the
	// fallback timestamp is captured when the response callback is delivered.
	return !nativeAt.After(fallbackAt.Add(time.Second))
}

func usageFallbackLateNativeRequestTimeCompatible(nativeAt time.Time, fallback usageFallbackOccurrence) bool {
	if usageFallbackNativeRequestTimeCompatible(nativeAt, fallback.requestAt) {
		return true
	}
	if nativeAt.IsZero() || fallback.observedAt.IsZero() {
		return true
	}
	// A fallback is timestamped when the response interceptor callback runs,
	// but it is committed after the fallback delay. Native usage can therefore
	// carry the handoff/completion time, which is later than the fallback's
	// RequestedAt even though both callbacks belong to the same request.
	return !nativeAt.After(fallback.observedAt.Add(usageFallbackNativeRecentWindow))
}

func usageFallbackClientAPIKey(record UsageRecord) string {
	key := canonicalClientAPIKey(record.APIKey)
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "", "unknown", "(unknown)", "unknown api", "\u672a\u77e5", "\u672a\u77e5 api":
		return ""
	default:
		return key
	}
}

func usageFallbackStreamKey(record UsageRecord) string {
	correlationID := strings.TrimSpace(record.correlationID)
	if correlationID == "" || len(correlationID) > maxUsageCorrelationIDLength {
		return ""
	}
	return strings.Join([]string{
		"stream",
		strings.ToLower(strings.TrimSpace(usageProviderFamily(record.Provider))),
		strings.ToLower(strings.TrimSpace(record.AuthID)),
		usageFallbackClientAPIKey(record),
		correlationID,
	}, "\x00")
}

// usageFallbackRecordsCompatible keeps the client API key as a strict
// discriminator when both callbacks provide it, while treating a missing key
// on either side as unknown metadata that can be enriched later.
func usageFallbackRecordsCompatible(left, right UsageRecord) bool {
	leftCorrelationID := strings.TrimSpace(left.correlationID)
	rightCorrelationID := strings.TrimSpace(right.correlationID)
	if leftCorrelationID != "" && rightCorrelationID != "" && leftCorrelationID != rightCorrelationID {
		return false
	}
	if !usageDetailHasTokens(left.Detail) || !usageDetailHasTokens(right.Detail) {
		return true
	}
	if usageFallbackMatchKey(left) != usageFallbackMatchKey(right) {
		return false
	}
	leftKey := usageFallbackClientAPIKey(left)
	rightKey := usageFallbackClientAPIKey(right)
	return leftKey == "" || rightKey == "" || leftKey == rightKey
}

func (c *usageFallbackCoordinator) evictOldestPendingLocked() {
	if c == nil {
		return
	}
	if len(c.deadlines) > 0 {
		item := heap.Pop(&c.deadlines).(*pendingUsageFallback)
		item.cancelled = true
		c.removePendingLocked(item)
		return
	}
	for key, items := range c.pending {
		for _, item := range items {
			if item == nil {
				continue
			}
			item.cancelled = true
			c.removePendingLocked(item)
			return
		}
		if len(items) == 0 {
			delete(c.pending, key)
		}
	}
}

func (c *usageFallbackCoordinator) evictOldestNativeRecentLocked() {
	if c == nil {
		return
	}
	for key, items := range c.nativeRecent {
		if len(items) == 0 {
			delete(c.nativeRecent, key)
			continue
		}
		if len(items) == 1 {
			delete(c.nativeRecent, key)
		} else {
			c.nativeRecent[key] = items[1:]
		}
		if c.nativeRecentCount > 0 {
			c.nativeRecentCount--
		}
		c.nativeRecentBytes -= items[0].bytes
		if c.nativeRecentBytes < 0 {
			c.nativeRecentBytes = 0
		}
		return
	}
}

func (c *usageFallbackCoordinator) evictOldestFallbackRecentLocked() {
	if c == nil {
		return
	}
	for key, items := range c.fallbackRecent {
		if len(items) == 0 {
			delete(c.fallbackRecent, key)
			continue
		}
		if len(items) == 1 {
			delete(c.fallbackRecent, key)
		} else {
			c.fallbackRecent[key] = items[1:]
		}
		if c.fallbackRecentCount > 0 {
			c.fallbackRecentCount--
		}
		c.fallbackRecentBytes -= items[0].bytes
		if c.fallbackRecentBytes < 0 {
			c.fallbackRecentBytes = 0
		}
		return
	}
}

const usageFallbackCleanupInterval = time.Second

func (c *usageFallbackCoordinator) retainedBytesLocked() int {
	if c == nil {
		return 0
	}
	return c.pendingBytes + c.nativeRecentBytes + c.fallbackRecentBytes
}

func usageFallbackRecordBytes(record UsageRecord) int {
	bytes := 256 + len(record.Provider) + len(record.ExecutorType) + len(record.Model) +
		len(record.Alias) + len(record.APIKey) + len(record.AuthID) + len(record.AuthIndex) +
		len(record.AuthType) + len(record.Endpoint) + len(record.BaseURL) + len(record.Source) +
		len(record.ReasoningEffort) + len(record.ServiceTier) + len(record.correlationID)
	for key, values := range record.ResponseHeaders {
		bytes += len(key) + 32
		for _, value := range values {
			bytes += len(value)
		}
	}
	if bytes < 256 {
		return 256
	}
	return bytes
}

func (c *usageFallbackCoordinator) cleanupMaybeLocked(now time.Time) {
	if c == nil {
		return
	}
	if !c.lastCleanup.IsZero() && now.Sub(c.lastCleanup) < usageFallbackCleanupInterval {
		return
	}
	c.cleanupLocked(now)
	c.lastCleanup = now
}

func (c *usageFallbackCoordinator) cleanupLocked(now time.Time) {
	for key, items := range c.nativeRecent {
		kept := items[:0]
		removedBytes := 0
		for _, item := range items {
			if now.Sub(item.observedAt) <= usageFallbackNativeRecentWindow {
				kept = append(kept, item)
			} else {
				removedBytes += item.bytes
			}
		}
		if len(kept) == 0 {
			delete(c.nativeRecent, key)
		} else {
			c.nativeRecent[key] = kept
		}
		c.nativeRecentCount -= len(items) - len(kept)
		c.nativeRecentBytes -= removedBytes
	}
	for key, items := range c.fallbackRecent {
		kept := items[:0]
		removedBytes := 0
		for _, item := range items {
			if now.Sub(item.observedAt) <= usageFallbackLateNativeWindow {
				kept = append(kept, item)
			} else {
				removedBytes += item.bytes
			}
		}
		if len(kept) == 0 {
			delete(c.fallbackRecent, key)
		} else {
			c.fallbackRecent[key] = kept
		}
		c.fallbackRecentCount -= len(items) - len(kept)
		c.fallbackRecentBytes -= removedBytes
	}
	for key, items := range c.pending {
		kept := items[:0]
		removed := 0
		removedBytes := 0
		for _, item := range items {
			if item != nil && !item.cancelled {
				kept = append(kept, item)
			} else if item != nil {
				c.removePendingHeapLocked(item)
				removed++
				removedBytes += item.bytes
			}
		}
		if len(kept) == 0 {
			delete(c.pending, key)
		} else {
			c.pending[key] = kept
		}
		c.pendingCount -= removed
		c.pendingBytes -= removedBytes
	}
	if c.nativeRecentCount < 0 {
		c.nativeRecentCount = 0
	}
	if c.fallbackRecentCount < 0 {
		c.fallbackRecentCount = 0
	}
	if c.pendingCount < 0 {
		c.pendingCount = 0
	}
	if c.nativeRecentBytes < 0 {
		c.nativeRecentBytes = 0
	}
	if c.fallbackRecentBytes < 0 {
		c.fallbackRecentBytes = 0
	}
	if c.pendingBytes < 0 {
		c.pendingBytes = 0
	}
}

// usageRecordFingerprint keys native/fallback dedup. Provider is collapsed to
// its family for recognized auth IDs so a fallback still matches legacy native
// records that omitted auth_id. When a file-backed auth uses a custom filename
// whose provider cannot be inferred by the interceptor, auth ID becomes the
// upstream identity; both modern native records and interceptor metadata carry
// that scheduler-selected ID. This keeps custom file auths deduplicated without
// regressing older records. A fallback that only knows the generic
// "openai-compatible" upstream still matches the native record's specific
// "openai-compatible-<name>" provider. Token counts are canonicalized to
// cache-inclusive input with a recomputed total: Claude-family records keep
// input exclusive of cache reads/creations (both CPA's native parser and the
// fallback's Claude-format normalization do), while every other family
// already folds cache into input — adding the cache fields for Claude-family
// records makes the same request produce one fingerprint no matter which
// side, protocol shape, or total_tokens convention reported it. The requested
// model alias is preferred over the upstream response model:
// native records carry the client-facing alias while fallback responses may
// expose a routed model name (for example grok-4.5-build-free for a grok-4.5
// request). Using the routed name would let the same request through twice and
// create a mirror dashboard group. Reasoning effort and service tier are
// deliberately excluded: the two sides derive them from different sources and
// the token triple already discriminates requests.
func usageRecordFingerprint(record UsageRecord) string {
	return usageRecordFingerprintWithClientAPI(record, true)
}

// usageFallbackMatchKey is the correlation key shared by native and
// interceptor callbacks. Client API identity is checked separately because
// one callback may omit it even though the other callback has it.
func usageFallbackMatchKey(record UsageRecord) string {
	return usageRecordFingerprintWithClientAPI(record, false)
}

func usageRecordFingerprintWithClientAPI(record UsageRecord, includeClientAPI bool) string {
	if !usageDetailHasTokens(record.Detail) {
		return ""
	}
	providerFamily := usageProviderFamily(record.Provider)
	upstreamIdentity := providerFamily
	if authID := strings.ToLower(strings.TrimSpace(record.AuthID)); authID != "" && providerFromAuthID(authID) == "" {
		upstreamIdentity = "auth:" + authID
	}
	inputTokens := record.Detail.InputTokens
	if providerFamily == "claude" {
		inputTokens += record.Detail.CacheReadTokens + record.Detail.CacheCreationTokens
	}
	outputTokens := record.Detail.OutputTokens
	parts := []string{
		upstreamIdentity,
		strings.ToLower(strings.TrimSpace(firstNonEmpty(record.Alias, record.Model))),
	}
	if includeClientAPI {
		parts = append(parts, usageFallbackClientAPIKey(record))
	}
	parts = append(parts,
		fmt.Sprintf("%d", inputTokens),
		fmt.Sprintf("%d", outputTokens),
		fmt.Sprintf("%d", inputTokens+outputTokens),
	)
	return strings.Join(parts, "\x00")
}

func usageProviderFamily(provider string) string {
	value := strings.ToLower(strings.TrimSpace(provider))
	switch {
	case value == "":
		return ""
	case value == "openai" || value == "openai-response" || value == "openai-responses" ||
		strings.HasPrefix(value, "openai-compatible") || strings.HasPrefix(value, "openai-compatibility"):
		return "openai-compatible"
	case value == "anthropic" || strings.HasPrefix(value, "anthropic-") ||
		value == "claude" || strings.HasPrefix(value, "claude-"):
		return "claude"
	case value == "gemini" || value == "aistudio" || value == "vertex" || value == "google" ||
		value == "geminicli" || value == "antigravity" || strings.HasPrefix(value, "gemini-") ||
		strings.HasPrefix(value, "aistudio-") || strings.HasPrefix(value, "vertex-") ||
		strings.HasPrefix(value, "geminicli-") || strings.HasPrefix(value, "antigravity-") ||
		strings.HasPrefix(value, "antigravity.") || strings.HasPrefix(value, "antigravity_"):
		return "gemini"
	default:
		return value
	}
}

func fallbackUsageProvider(req ResponseInterceptRequest) string {
	source := strings.ToLower(strings.TrimSpace(req.SourceFormat))
	switch {
	case source == "openai" || source == "openai-response" || source == "openai-responses":
		return "openai-compatible"
	case strings.Contains(source, "antigravity"):
		return "antigravity"
	case strings.Contains(source, "gemini"):
		return "gemini"
	case strings.Contains(source, "claude") || strings.Contains(source, "anthropic"):
		return "claude"
	case source != "":
		return strings.TrimSpace(req.SourceFormat)
	default:
		return "openai-compatible"
	}
}

func providerFromSelectedAuthID(meta map[string]any) string {
	authID := metadataString(meta, "selected_auth_id", "pinned_auth_id")
	return providerFromAuthID(authID)
}

func providerFromAuthID(authID string) string {
	value := strings.TrimSpace(authID)
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	parts := strings.Split(value, ":")
	switch strings.ToLower(strings.TrimSpace(parts[0])) {
	case "openai-compatibility":
		if len(parts) < 2 {
			return "openai-compatible"
		}
		name := strings.TrimSpace(parts[1])
		if name != "" {
			return "openai-compatible-" + name
		}
	case "codex":
		return "codex"
	}
	if strings.HasPrefix(lower, "codex-") || strings.HasPrefix(lower, "codex_") || strings.HasPrefix(lower, "codex.") {
		return "codex"
	}
	// File-backed auth IDs are normally their JSON filenames. Generated OAuth
	// and Vertex credentials use a provider prefix, and nested auth directories
	// may leave a relative path in the ID. Recognize every native file-backed
	// provider registered by CPA instead of falling back to the client protocol.
	fileID := lower
	if index := strings.LastIndexAny(fileID, "/\\"); index >= 0 {
		fileID = fileID[index+1:]
	}
	switch {
	case authIDHasProviderPrefix(fileID, "claude"), authIDHasProviderPrefix(fileID, "anthropic"):
		return "claude"
	case authIDHasProviderPrefix(fileID, "kimi"):
		return "kimi"
	case authIDHasProviderPrefix(fileID, "xai"), authIDHasProviderPrefix(fileID, "grok"):
		return "xai"
	case authIDHasProviderPrefix(fileID, "vertex"):
		return "vertex"
	case authIDHasProviderPrefix(fileID, "aistudio"):
		return "aistudio"
	case authIDHasProviderPrefix(fileID, "antigravity"):
		return "antigravity"
	case authIDHasProviderPrefix(fileID, "gemini"), authIDHasProviderPrefix(fileID, "geminicli"):
		return "gemini"
	}
	return ""
}

// authIDHasProviderPrefix checks whether authID starts with provider followed by a
// recognised separator (-, _, ., :). Note: '@' is deliberately not included as a
// separator because CPA-generated file auth IDs always use '-' (e.g.
// claude-user@example.com.json). An auth ID like claude@example.com.json (without a
// trailing '-' after the provider) will not match — this is safe by construction.
func authIDHasProviderPrefix(authID, provider string) bool {
	if authID == provider {
		return true
	}
	if !strings.HasPrefix(authID, provider) || len(authID) <= len(provider) {
		return false
	}
	switch authID[len(provider)] {
	case '-', '_', '.', ':':
		return true
	default:
		return false
	}
}

func fallbackAuthIndex(meta map[string]any, authID string) string {
	if index := metadataString(meta, "auth_index", "selected_auth_index", "pinned_auth_index"); index != "" {
		return index
	}
	if learned := authIndexes.Lookup(authID); learned != "" {
		return learned
	}
	return safeCredentialIdentity(authID)
}

func fallbackAuthType(meta map[string]any, authID string) string {
	if authType := metadataString(meta, "auth_type", "selected_auth_type", "pinned_auth_type"); authType != "" {
		return authType
	}
	value := strings.TrimSpace(authID)
	parts := strings.Split(value, ":")
	if len(parts) == 0 {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(parts[0])) {
	case "openai-compatibility":
		return "apikey"
	case "codex":
		if len(parts) >= 2 && strings.EqualFold(strings.TrimSpace(parts[1]), "apikey") {
			return "apikey"
		}
		return "oauth"
	default:
		provider := providerFromAuthID(authID)
		switch provider {
		case "claude", "codex", "kimi", "xai", "aistudio", "antigravity", "gemini", "vertex":
			return "oauth"
		}
		return ""
	}
}

func apiKeyFromHeaders(headers map[string][]string) string {
	auth := headerValue(headers, "Authorization")
	if auth != "" {
		fields := strings.Fields(auth)
		if len(fields) == 2 && strings.EqualFold(fields[0], "bearer") {
			return strings.TrimSpace(fields[1])
		}
		return strings.TrimSpace(auth)
	}
	return firstNonEmpty(
		headerValue(headers, "X-API-Key"),
		headerValue(headers, "X-Api-Key"),
		headerValue(headers, "X-Goog-Api-Key"),
	)
}

func headerValue(headers map[string][]string, name string) string {
	if len(headers) == 0 {
		return ""
	}
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, value := range values {
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func decodeJSONValue(data []byte) (any, bool) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, false
	}
	return value, true
}

func jsonValuePath(root any, path string) (any, bool) {
	if root == nil || strings.TrimSpace(path) == "" {
		return nil, false
	}
	current := root
	for _, part := range strings.Split(path, ".") {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := m[part]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func jsonStringPath(root any, path string) string {
	value, ok := jsonValuePath(root, path)
	if !ok {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return strings.TrimSpace(v.String())
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func jsonStringPathFromValues(values []any, paths ...string) string {
	for _, value := range values {
		for _, path := range paths {
			if got := jsonStringPath(value, path); got != "" {
				return got
			}
		}
	}
	return ""
}

func metadataString(meta map[string]any, keys ...string) string {
	if len(meta) == 0 {
		return ""
	}
	for _, key := range keys {
		value, ok := meta[key]
		if !ok {
			continue
		}
		if value := metadataValueString(value); value != "" {
			return value
		}
	}
	return ""
}

func metadataValueString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	case json.Number:
		return strings.TrimSpace(v.String())
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func firstHeaderMap(values ...map[string][]string) map[string][]string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstBytes(values ...[]byte) []byte {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstJSONInt(m map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			if n := jsonInt(value); n != 0 {
				return n
			}
		}
	}
	return 0
}

// firstNestedJSONInt reads key from the first of the given child objects that
// carries it, e.g. usage.prompt_tokens_details.cached_tokens.
func firstNestedJSONInt(m map[string]any, key string, parents ...string) int64 {
	for _, parent := range parents {
		child, ok := m[parent].(map[string]any)
		if !ok {
			continue
		}
		if n := firstJSONInt(child, key); n != 0 {
			return n
		}
	}
	return 0
}

func jsonInt(value any) int64 {
	switch v := value.(type) {
	case json.Number:
		n, _ := v.Int64()
		return n
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n
	default:
		return 0
	}
}
