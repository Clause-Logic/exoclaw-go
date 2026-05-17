package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Ported from tests/test_http.py.

func TestParseURL_HTTPSDefaultPort(t *testing.T) {
	scheme, host, port, path, err := ParseURL("https://example.com/foo?q=1")
	if err != nil {
		t.Fatal(err)
	}
	if scheme != "https" || host != "example.com" || port != 443 || path != "/foo?q=1" {
		t.Fatalf("got %s %s %d %s", scheme, host, port, path)
	}
}

func TestParseURL_HTTPCustomPort(t *testing.T) {
	scheme, host, port, path, err := ParseURL("http://localhost:8080/")
	if err != nil {
		t.Fatal(err)
	}
	if scheme != "http" || host != "localhost" || port != 8080 || path != "/" {
		t.Fatalf("got %s %s %d %s", scheme, host, port, path)
	}
}

func TestParseURL_BadScheme(t *testing.T) {
	if _, _, _, _, err := ParseURL("ftp://x"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseURL_RootPathDefault(t *testing.T) {
	_, _, _, path, err := ParseURL("https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if path != "/" {
		t.Fatalf("path: %q", path)
	}
}

func TestPostJSON_RoundTrip(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request: %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"reply":"ok"}`))
	}))
	defer ts.Close()

	client := New(2 * time.Second)
	out, err := PostJSON(context.Background(), client, ts.URL, map[string]any{"q": "hi"}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("not a map: %T", out)
	}
	if m["reply"] != "ok" {
		t.Fatalf("reply: %v", m["reply"])
	}
}

func TestStreamPost_StatusError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte("upstream"))
	}))
	defer ts.Close()

	client := New(0)
	resp, cleanup, err := client.StreamPost(context.Background(), ts.URL, StreamPostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if _, err := resp.ARead(); err != nil {
		t.Fatal(err)
	}
	err = resp.RaiseForStatus()
	var se *HTTPStatusError
	if !errors.As(err, &se) {
		t.Fatalf("expected status error, got %T", err)
	}
	if se.StatusCode != 503 {
		t.Fatalf("code: %d", se.StatusCode)
	}
}

func TestStreamPost_IterLines(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "line1\nline2\nline3\n")
	}))
	defer ts.Close()

	client := New(0)
	resp, cleanup, err := client.StreamPost(context.Background(), ts.URL, StreamPostOptions{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	lines, errs := resp.IterLines()
	var got []string
	for s := range lines {
		got = append(got, s)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "line1" || got[2] != "line3" {
		t.Fatalf("got %v", got)
	}
}
