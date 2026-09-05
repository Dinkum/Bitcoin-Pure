package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/types"
)

type responseCountingBody struct {
	reader io.Reader
	read   int64
	closed bool
}

func (b *responseCountingBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	return n, err
}
func (b *responseCountingBody) Close() error { b.closed = true; return nil }

type responseFiller struct{}

func (responseFiller) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestRPCResponseLimits(t *testing.T) {
	limit := rpcResponseLimit("getinfo")
	for _, test := range []struct {
		name          string
		extra         int64
		contentLength int64
		wantError     bool
	}{
		{"exact-limit", 0, limit, false}, {"chunked-overflow", 1, -1, true}, {"misleading-length", 1, 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			prefix, suffix := `{"result":"`, `"}`
			body := &responseCountingBody{reader: io.MultiReader(strings.NewReader(prefix), io.LimitReader(responseFiller{}, limit-int64(len(prefix)+len(suffix))+test.extra), strings.NewReader(suffix))}
			client := newRPCClient("http://limit.invalid", "", time.Second)
			client.http.Transport = statusTestTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: body, ContentLength: test.contentLength, Header: make(http.Header)}, nil
			})
			err := client.Call("getinfo", nil, nil)
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "response exceeds") {
					t.Fatalf("expected limit error, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !body.closed || body.read > limit+1 {
				t.Fatalf("unbounded or unclosed body: bytes=%d closed=%v", body.read, body.closed)
			}
		})
	}
	if rpcResponseLimit("getblock") < 2*int64(types.DefaultCodecLimits().MaxBlockBytes) {
		t.Fatal("block response budget excludes valid hex blocks")
	}
	if rpcResponseLimit("getutxosbywatchitems") <= limit {
		t.Fatal("bulk wallet queries need a larger bounded budget")
	}
}

func TestRPCErrorResponseIsTruncated(t *testing.T) {
	body := &responseCountingBody{reader: responseFiller{}}
	client := newRPCClient("http://limit.invalid", "", time.Second)
	client.http.Transport = statusTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 502, Status: "502 Bad Gateway", Body: body, Header: make(http.Header)}, nil
	})
	err := client.Call("getblock", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "[truncated]") || body.read != rpcErrorBodyLimit+1 || !body.closed {
		t.Fatalf("error body was not bounded: %v bytes=%d", err, body.read)
	}
}

func TestRPCRedirectDoesNotForwardToken(t *testing.T) {
	forwarded := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = true
		_, _ = io.WriteString(w, `{"result":{}}`)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client := newRPCClient(origin.URL, "private-test-token", time.Second)
	if err := client.Call("getinfo", nil, nil); err == nil {
		t.Fatal("redirect accepted as RPC success")
	}
	if forwarded {
		t.Fatal("followed authenticated RPC redirect")
	}
}
