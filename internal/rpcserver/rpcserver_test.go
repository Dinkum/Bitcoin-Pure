package rpcserver

import (
	"encoding/json"
	"strings"
	"testing"
)

type testService struct {
	seen string
}

func TestDispatchCallsTypedHandler(t *testing.T) {
	registry := Registry[testService]{
		"echo": RequiredParams[testService](func(s *testService, params struct {
			Value string `json:"value"`
		}) (string, error) {
			s.seen = params.Value
			return params.Value, nil
		}),
	}
	service := &testService{}

	result, err := Dispatch(service, registry, Request{
		Method: "echo",
		Params: json.RawMessage(`{"value":"ok"}`),
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if result != "ok" {
		t.Fatalf("result = %v, want ok", result)
	}
	if service.seen != "ok" {
		t.Fatalf("service saw %q, want ok", service.seen)
	}
}

func TestDispatchRejectsUnknownMethod(t *testing.T) {
	_, err := Dispatch(&testService{}, Registry[testService]{}, Request{Method: "missing"})
	if err == nil || !strings.Contains(err.Error(), "unknown rpc method: missing") {
		t.Fatalf("error = %v, want unknown method", err)
	}
}

func TestRequiredParamsRejectsMissingParams(t *testing.T) {
	handler := RequiredParams[testService](func(_ *testService, _ struct {
		Value string `json:"value"`
	}) (bool, error) {
		return true, nil
	})

	if _, err := handler(&testService{}, nil); err == nil {
		t.Fatal("expected missing params to fail")
	}
}

func TestOptionalParamsAllowsMissingParams(t *testing.T) {
	handler := OptionalParams[testService](func(_ *testService, params struct {
		TargetBlocks int `json:"target_blocks"`
	}) (int, error) {
		return params.TargetBlocks, nil
	})

	result, err := handler(&testService{}, nil)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if result != 0 {
		t.Fatalf("result = %v, want default zero", result)
	}
}
