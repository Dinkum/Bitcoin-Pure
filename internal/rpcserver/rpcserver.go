package rpcserver

import (
	"encoding/json"
	"fmt"
)

type Request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type Response struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

type Handler[S any] func(*S, json.RawMessage) (any, error)

type Registry[S any] map[string]Handler[S]

func Dispatch[S any](target *S, registry Registry[S], req Request) (any, error) {
	handler, ok := registry[req.Method]
	if !ok {
		return nil, fmt.Errorf("unknown rpc method: %s", req.Method)
	}
	return handler(target, req.Params)
}

func NoParams[S any, R any](fn func(*S) (R, error)) Handler[S] {
	return func(s *S, _ json.RawMessage) (any, error) {
		return fn(s)
	}
}

func RequiredParams[S any, P any, R any](fn func(*S, P) (R, error)) Handler[S] {
	return func(s *S, raw json.RawMessage) (any, error) {
		params, err := decodeParams[P](raw)
		if err != nil {
			var zero R
			return zero, err
		}
		return fn(s, params)
	}
}

func OptionalParams[S any, P any, R any](fn func(*S, P) (R, error)) Handler[S] {
	return func(s *S, raw json.RawMessage) (any, error) {
		params, err := decodeOptionalParams[P](raw)
		if err != nil {
			var zero R
			return zero, err
		}
		return fn(s, params)
	}
}

func decodeParams[P any](raw json.RawMessage) (P, error) {
	var params P
	if err := json.Unmarshal(raw, &params); err != nil {
		return params, err
	}
	return params, nil
}

func decodeOptionalParams[P any](raw json.RawMessage) (P, error) {
	var params P
	if len(raw) == 0 {
		return params, nil
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return params, err
	}
	return params, nil
}
