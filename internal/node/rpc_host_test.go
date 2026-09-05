package node

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTokenFreeRPCHostBoundary(t *testing.T) {
	for _, test := range []struct {
		host    string
		allowed bool
	}{
		{"127.0.0.1:18443", true}, {"127.0.0.2", true}, {"[::1]:18443", true}, {"[::ffff:127.0.0.1]:18443", true},
		{"localhost:18443", true}, {"LOCALHOST.", true},
		{"attacker.invalid:18443", false}, {"localhost.attacker.invalid", false}, {"192.168.1.1", false}, {"[2001:db8::1]", false},
		{"localhost:0", false}, {"localhost:65536", false}, {"localhost:", false}, {"localhost:abc", false},
		{"user@localhost", false}, {"localhost/path", false}, {"localhost?x", false}, {"localhost#x", false}, {"[::1%25lo0]", false}, {"", false},
	} {
		t.Run(test.host, func(t *testing.T) {
			if got := isLoopbackRPCHost(test.host); got != test.allowed {
				t.Fatalf("host allowed=%v want %v", got, test.allowed)
			}
		})
	}
	for _, origin := range []string{"", "http://attacker.invalid:18443"} {
		svc := &Service{logger: slog.Default()}
		req := httptest.NewRequest(http.MethodPost, "http://attacker.invalid:18443/", strings.NewReader(`{"method":"unknown","params":{}}`))
		req.RemoteAddr = "127.0.0.1:45000"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", origin)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		resp := httptest.NewRecorder()
		svc.handleRPC(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("attacker host reached dispatch: %d", resp.Code)
		}
	}
}

func TestTokenAuthenticatedRPCAllowsConfiguredRemoteHost(t *testing.T) {
	svc := &Service{cfg: ServiceConfig{RPCAuthToken: "test-token"}}
	req := httptest.NewRequest(http.MethodPost, "http://node.example:18443/", nil)
	if svc.authorizeRPC(req) {
		t.Fatal("missing token accepted")
	}
	req.Header.Set("Authorization", "Bearer test-token")
	if !svc.authorizeRPC(req) {
		t.Fatal("valid token rejected")
	}
}
