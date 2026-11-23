package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/wallet"
)

func TestLogTailSuffixMatchesLines(t *testing.T) {
	for i, data := range []string{"", "a", "a\n", "a\nb", "a\nb\n", "a\r\nb\r\n", "\n\n", strings.Repeat("x", 70000) + "\nb\n", strings.Repeat("x\n", 100000)} {
		for _, n := range []int{1, 2, 200, 100001} {
			p := filepath.Join(t.TempDir(), "log")
			if err := os.WriteFile(p, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readLastLogLines(p, n)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{}
			if data != "" {
				want = strings.Split(strings.TrimSuffix(data, "\n"), "\n")
				for j := range want {
					want[j] = strings.TrimSuffix(want[j], "\r")
				}
				if len(want) > n {
					want = want[len(want)-n:]
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fixture %d n=%d mismatch %d/%d", i, n, len(got), len(want))
			}
		}
	}
}

func TestFetchNodeStatusValuesAndErrors(t *testing.T) {
	results := map[string]string{"getinfo": `{"tip_height":42}`, "getmempoolinfo": `{"count":7}`, "getmininginfo": `{"enabled":true}`, "getpeerinfo": `[{"addr":"peer:1"}]`}
	for _, bad := range []string{"", "getinfo", "getmempoolinfo", "getmininginfo", "getpeerinfo"} {
		client := newRPCClient("http://status.invalid", "", time.Second)
		client.http.Transport = statusTestTransport(func(r *http.Request) (*http.Response, error) {
			var req struct {
				Method string `json:"method"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				return nil, err
			}
			body := `{"result":` + results[req.Method] + `}`
			if req.Method == bad {
				body = `{"error":"injected status failure"}`
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})
		got, err := fetchNodeStatus(client)
		if bad != "" {
			if err == nil || !strings.Contains(err.Error(), "injected status failure") {
				t.Fatalf("%s: %v", bad, err)
			}
			continue
		}
		if err != nil || got.Info.TipHeight != 42 || got.Mempool.Count != 7 || !got.Mining.Enabled || len(got.Peers) != 1 || got.Peers[0].Addr != "peer:1" {
			t.Fatalf("result mismatch: %+v %v", got, err)
		}
	}
}

func TestFetchNodeStatusCancelsOutstandingCalls(t *testing.T) {
	client := newRPCClient("http://status.invalid", "", time.Second)
	client.http.Transport = statusTestTransport(func(r *http.Request) (*http.Response, error) {
		var req struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		if req.Method == "getpeerinfo" {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"error":"injected status failure"}`)), Header: make(http.Header)}, nil
		}
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-time.After(time.Second):
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"result":null}`)), Header: make(http.Header)}, nil
		}
	})
	start := time.Now()
	_, err := fetchNodeStatus(client)
	if err == nil || !strings.Contains(err.Error(), "injected status failure") {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("outstanding requests were not canceled")
	}
}

type statusTestTransport func(*http.Request) (*http.Response, error)

func (f statusTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLogTailEnforcesSelectedLineBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	data := strings.Repeat("x", maxLogLineBytes) + "\nrecent\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readLastLogLines(path, 1)
	if err != nil || !reflect.DeepEqual(got, []string{"recent"}) {
		t.Fatalf("tail = %v, %v", got, err)
	}
	if _, err := readLastLogLines(path, 2); err == nil {
		t.Fatal("oversized selected line accepted")
	}
	if _, err := readLastLogLines(path, 0); err == nil {
		t.Fatal("zero line count accepted")
	}
}

func TestLogTailDetectsTruncationDuringReverseRead(t *testing.T) {
	if _, err := logTailStart(strings.NewReader("a"), 2, 1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v", err)
	}
}

func TestLogTailAtBufferBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	for _, size := range []int{65535, 65536, 65537, 131071, 131072, 131073} {
		for _, suffix := range []string{"\nlast", "\nlast\n", "\r\nlast\r\n"} {
			if err := os.WriteFile(path, []byte(strings.Repeat("x", size)+suffix), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readLastLogLines(path, 1)
			if err != nil || !reflect.DeepEqual(got, []string{"last"}) {
				t.Fatalf("size %d suffix %q: %v, %v", size, suffix, got, err)
			}
		}
	}
}

func TestSyncWalletStateSkipsPendingRPCsAndMutationWithoutPending(t *testing.T) {
	store, err := wallet.Open(wallet.StorePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateWallet("main"); err != nil {
		t.Fatal(err)
	}
	// A read-only balance must not attempt to acquire a mutation lock.
	lockPath := store.Path() + ".lock"
	if err := os.Rename(lockPath, lockPath+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lockPath, 0700); err != nil {
		t.Fatal(err)
	}
	client := newRPCClient("http://wallet.invalid", "", time.Second)
	var methods []string
	client.http.Transport = statusTestTransport(func(r *http.Request) (*http.Response, error) {
		var req struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		methods = append(methods, req.Method)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"result":{"utxos":[]}}`))}, nil
	})
	if _, _, err := syncWalletState(store, client, "main", 0); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(methods, []string{"getutxosbywatchitems"}) {
		t.Fatalf("RPCs = %v, want only getutxosbywatchitems", methods)
	}
}

func TestWalletActivityMergesAcrossWatchItemBatches(t *testing.T) {
	client := newRPCClient("http://wallet.invalid", "", time.Second)
	items := make([]wallet.WatchItem, node.MaxRPCWatchItems+1)
	for i := range items {
		items[i].Payload32[0] = byte(i)
		items[i].Payload32[1] = byte(i >> 8)
	}
	var requests int
	client.http.Transport = statusTestTransport(func(r *http.Request) (*http.Response, error) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				WatchItems []json.RawMessage `json:"watchitems"`
				Limit      int               `json:"limit"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		requests++
		wantSize := node.MaxRPCWatchItems
		body := `{"result":{"activity":[{"txid":"01","block_hash":"00","height":2,"received":10,"fee":2},{"txid":"02","height":1}]}}`
		if requests == 2 {
			wantSize = 1
			body = `{"result":{"activity":[{"txid":"01","block_hash":"00","height":2,"sent":6,"fee":2}]}}`
		}
		if req.Method != "getwalletactivitybywatchitems" || len(req.Params.WatchItems) != wantSize || req.Params.Limit != 2 {
			t.Fatalf("bad batch request %d: %s, %d items, limit %d", requests, req.Method, len(req.Params.WatchItems), req.Params.Limit)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	rows, err := rpcWalletActivityByWatchItems(client, items, 2)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(rows) != 2 || rows[0].Net != 4 || rows[0].Fee != 2 || rows[1].TxID != "02" {
		t.Fatalf("got %d requests and rows %+v", requests, rows)
	}
}
