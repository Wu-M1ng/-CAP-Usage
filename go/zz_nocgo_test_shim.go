//go:build !cgo

package main

import "encoding/json"

func okEnvelopeJSON(result string) ([]byte, error) {
	return json.Marshal(envelope{OK: true, Result: json.RawMessage(result)})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

func mustMarshal(value interface{}) []byte {
	data, _ := json.Marshal(value)
	return data
}
