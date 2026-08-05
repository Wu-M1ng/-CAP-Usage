package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

type usageDecodeMode uint8

const (
	usageDecodeComplete usageDecodeMode = iota
	usageDecodeStream
)

type decodedUsage struct {
	model         string
	correlationID string
	detail        UsageDetail
}

// forEachSSEData visits each independent data line without converting the
// complete response to a string or allocating a slice of lines. The callback
// may return false to stop after the first useful usage payload.
func forEachSSEData(body []byte, visit func([]byte) bool) {
	for len(body) > 0 {
		line, rest, found := bytes.Cut(body, []byte{'\n'})
		if !found {
			rest = nil
		}
		body = rest
		line = bytes.TrimSuffix(line, []byte{'\r'})
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			if !found {
				return
			}
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			if !found {
				return
			}
			continue
		}
		if !visit(data) {
			return
		}
		if !found {
			return
		}
	}
}

func decodeUsagePayload(body []byte, mode usageDecodeMode) (decodedUsage, bool) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return decodedUsage{}, false
	}
	var best decodedUsage
	found := false
	consider := func(candidate decodedUsage) {
		if !usageDetailHasTokens(candidate.detail) {
			return
		}
		if !found || usageDetailCompleteness(candidate.detail) >= usageDetailCompleteness(best.detail) {
			best = candidate
			found = true
		}
	}
	if body[0] == '{' || body[0] == '[' {
		if candidate, ok := decodeUsageJSONValue(body, mode); ok {
			consider(candidate)
		}
		return best, found
	}
	forEachSSEData(body, func(data []byte) bool {
		candidate, ok := decodeUsageJSONValue(data, mode)
		if ok {
			consider(candidate)
		}
		return true
	})
	return best, found
}

func decodeUsageJSONValue(raw []byte, mode usageDecodeMode) (decodedUsage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var result decodedUsage
	if err := scanUsageJSONValue(decoder, mode, &result, false, 0); err != nil {
		return decodedUsage{}, false
	}
	if !usageDetailHasTokens(result.detail) {
		return decodedUsage{}, false
	}
	return result, true
}

func scanUsageJSONValue(decoder *json.Decoder, mode usageDecodeMode, result *decodedUsage, inMessage bool, depth int) error {
	if depth > 32 {
		return skipJSONDecoderValue(decoder)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return io.ErrUnexpectedEOF
			}
			switch normalizeUsageFieldName(key) {
			case "usage", "totalusage", "usagemetadata":
				if mode == usageDecodeStream && inMessage {
					if err := skipJSONDecoderValue(decoder); err != nil {
						return err
					}
					continue
				}
				var detail UsageDetail
				if err := decodeUsageDetailObject(decoder, &detail); err != nil {
					return err
				}
				if usageDetailCompleteness(detail) >= usageDetailCompleteness(result.detail) {
					result.detail = detail
				}
			case "model":
				var model string
				if err := decoder.Decode(&model); err != nil {
					return err
				}
				if result.model == "" {
					result.model = model
				}
			case "id", "responseid", "requestid", "streamid":
				id, err := decodeJSONScalarString(decoder)
				if err != nil {
					return err
				}
				if result.correlationID == "" {
					result.correlationID = id
				}
			case "message":
				if err := scanUsageJSONValue(decoder, mode, result, true, depth+1); err != nil {
					return err
				}
			case "response":
				if err := scanUsageJSONValue(decoder, mode, result, inMessage, depth+1); err != nil {
					return err
				}
			default:
				if err := skipJSONDecoderValue(decoder); err != nil {
					return err
				}
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanUsageJSONValue(decoder, mode, result, inMessage, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return nil
	}
}

func decodeUsageDetailObject(decoder *json.Decoder, detail *UsageDetail) error {
	if detail == nil {
		return skipJSONDecoderValue(decoder)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		normalized := normalizeUsageFieldName(key)
		switch normalized {
		case "prompttokens", "inputtokens", "totalinputtokens", "prompttokencount":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.InputTokens == 0 {
				detail.InputTokens = value
			}
		case "completiontokens", "outputtokens", "totaloutputtokens", "candidatestokencount":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.OutputTokens == 0 {
				detail.OutputTokens = value
			}
		case "reasoningtokens", "totalthoughttokens", "thoughtstokencount":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.ReasoningTokens == 0 {
				detail.ReasoningTokens = value
			}
		case "cachedtokens", "totalcachedtokens", "cachedcontenttokencount":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.CachedTokens == 0 {
				detail.CachedTokens = value
			}
			if detail.CacheReadTokens == 0 {
				detail.CacheReadTokens = value
			}
		case "cachereadtokens", "cachereadinputtokens":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.CacheReadTokens == 0 {
				detail.CacheReadTokens = value
			}
		case "cachecreationtokens", "cachecreationinputtokens", "cachewritetokens":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.CacheCreationTokens == 0 {
				detail.CacheCreationTokens = value
			}
		case "totaltokens", "totaltokencount":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.TotalTokens == 0 {
				detail.TotalTokens = value
			}
		case "prompttokensdetails", "inputtokensdetails", "completiontokensdetails", "cacheusage":
			if err := decodeUsageDetailNestedObject(decoder, detail); err != nil {
				return err
			}
		default:
			if err := skipJSONDecoderValue(decoder); err != nil {
				return err
			}
		}
	}
	_, err = decoder.Token()
	if detail.TotalTokens == 0 {
		detail.TotalTokens = detail.InputTokens + detail.OutputTokens
	}
	return err
}

func decodeUsageDetailNestedObject(decoder *json.Decoder, detail *UsageDetail) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		switch normalizeUsageFieldName(key) {
		case "cachedtokens":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.CachedTokens == 0 {
				detail.CachedTokens = value
			}
			if detail.CacheReadTokens == 0 {
				detail.CacheReadTokens = value
			}
		case "cachecreationtokens", "cachewritetokens":
			value, err := decodeJSONInt64(decoder)
			if err != nil {
				return err
			}
			if detail.CacheCreationTokens == 0 {
				detail.CacheCreationTokens = value
			}
		default:
			if err := skipJSONDecoderValue(decoder); err != nil {
				return err
			}
		}
	}
	_, err = decoder.Token()
	return err
}

func decodeJSONInt64(decoder *json.Decoder) (int64, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, err
	}
	switch value := token.(type) {
	case json.Number:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err == nil {
			return parsed, nil
		}
		floatValue, floatErr := strconv.ParseFloat(string(value), 64)
		if floatErr != nil {
			return 0, err
		}
		return int64(floatValue), nil
	case float64:
		return int64(value), nil
	case string:
		parsed, parseErr := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if parseErr != nil {
			return 0, nil
		}
		return parsed, nil
	default:
		return 0, nil
	}
}

func decodeJSONScalarString(decoder *json.Decoder) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	switch value := token.(type) {
	case string:
		return value, nil
	case json.Number:
		return string(value), nil
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(value), nil
	case nil:
		return "", nil
	default:
		return "", nil
	}
}

func normalizeUsageFieldName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, "-", "")
	return value
}
