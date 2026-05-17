// Package http is a cross-platform async HTTP/1.1 client.
//
// Ported from exoclaw/http/__init__.py + exoclaw/http/_cpython.py. The
// MicroPython hand-rolled HTTP path is dropped — Go's net/http stdlib
// covers everything the Python _cpython.py path needs.
package http

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ----------------------------------------------------------------------
// Exceptions
// ----------------------------------------------------------------------

// HTTPError is the base for exoclaw/http transport errors.
type HTTPError struct{ Msg string }

func (e *HTTPError) Error() string {
	if e.Msg == "" {
		return "http error"
	}
	return e.Msg
}

// HTTPConnectError is a TCP / TLS connect failure (DNS, refused, handshake).
type HTTPConnectError struct{ HTTPError }

// HTTPReadTimeout is "server didn't send bytes within the timeout".
type HTTPReadTimeout struct{ HTTPError }

// HTTPWriteTimeout is "couldn't push request bytes within the timeout".
type HTTPWriteTimeout struct{ HTTPError }

// HTTPStatusError is a 4xx / 5xx response.
type HTTPStatusError struct {
	StatusCode int
	Msg        string
}

func (e *HTTPStatusError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("HTTP %d", e.StatusCode)
	}
	return e.Msg
}

// ----------------------------------------------------------------------
// Public interfaces
// ----------------------------------------------------------------------

// Response is the common surface for streaming responses.
type Response interface {
	StatusCode() int
	Headers() map[string]string
	ARead() ([]byte, error)
	Text() string
	RaiseForStatus() error
	IterLines() (<-chan string, <-chan error)
}

// StreamCM is the async context manager for one streaming POST.
//
// Use:
//
//	resp, cleanup, err := client.StreamPost(ctx, ...)
//	if err != nil { ... }
//	defer cleanup()
//
// The cleanup func releases the underlying body and is the Go equivalent of
// __aexit__ in the Python original.
type StreamCM = func()

// Client is the common surface for the async HTTP client.
type Client interface {
	Close() error
	StreamPost(ctx context.Context, url string, opts StreamPostOptions) (Response, StreamCM, error)
}

// StreamPostOptions bundles the optional kwargs for StreamPost.
type StreamPostOptions struct {
	Headers map[string]string
	Content []byte
	// ContentReader is the streaming alternative to Content; either may be
	// set, not both.
	ContentReader io.Reader
	Timeout       time.Duration
	Method        string // defaults to "POST"
}

// ----------------------------------------------------------------------
// URL parsing
// ----------------------------------------------------------------------

// ParseURL returns (scheme, host, port, path-with-query).
//
// Minimal URL parser — handles http:// / https:// URLs. No userinfo support,
// no IPv6 brackets.
func ParseURL(url string) (scheme, host string, port int, path string, err error) {
	var rest string
	var defaultPort int
	switch {
	case strings.HasPrefix(url, "https://"):
		scheme = "https"
		rest = url[8:]
		defaultPort = 443
	case strings.HasPrefix(url, "http://"):
		scheme = "http"
		rest = url[7:]
		defaultPort = 80
	default:
		return "", "", 0, "", fmt.Errorf("unsupported URL scheme: %q", url)
	}

	slash := strings.Index(rest, "/")
	var authority string
	if slash == -1 {
		authority = rest
		path = "/"
	} else {
		authority = rest[:slash]
		path = rest[slash:]
		if path == "" {
			path = "/"
		}
	}

	colon := strings.LastIndex(authority, ":")
	if colon == -1 {
		host = authority
		port = defaultPort
	} else {
		host = authority[:colon]
		var p int
		_, perr := fmt.Sscanf(authority[colon+1:], "%d", &p)
		if perr != nil {
			return "", "", 0, "", fmt.Errorf("bad port in %q: %w", url, perr)
		}
		port = p
	}
	return scheme, host, port, path, nil
}

// ----------------------------------------------------------------------
// Stdlib-backed Client + Response
// ----------------------------------------------------------------------

// stdClient implements Client over net/http with a configurable timeout.
type stdClient struct {
	httpClient *http.Client
	timeout    time.Duration
}

// New returns a runtime-appropriate async HTTP client. In the Go port this
// is always the stdlib net/http-backed implementation.
func New(timeout time.Duration) Client {
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return &stdClient{
		httpClient: &http.Client{Timeout: timeout},
		timeout:    timeout,
	}
}

// NewWithClient wraps a caller-supplied *http.Client. Used by tests
// that need to install a custom RoundTripper (e.g. a go-vcr Recorder
// replaying recorded HTTP interactions) without giving the provider
// any awareness of the test scaffolding.
//
// The caller owns the http.Client lifecycle — Close() on the wrapping
// xhttp.Client is a no-op. If the caller's http.Client already has a
// Timeout set, that wins; the `timeout` arg is the request-level
// fallback used by StreamPost when its own opts.Timeout is zero.
func NewWithClient(httpClient *http.Client, timeout time.Duration) Client {
	if httpClient == nil {
		return New(timeout)
	}
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return &stdClient{
		httpClient: httpClient,
		timeout:    timeout,
	}
}

func (c *stdClient) Close() error { return nil }

func (c *stdClient) StreamPost(ctx context.Context, url string, opts StreamPostOptions) (Response, StreamCM, error) {
	method := opts.Method
	if method == "" {
		method = "POST"
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = c.timeout
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)

	var body io.Reader
	switch {
	case opts.ContentReader != nil:
		body = opts.ContentReader
	case opts.Content != nil:
		body = bytes.NewReader(opts.Content)
	}

	req, err := http.NewRequestWithContext(rctx, method, url, body)
	if err != nil {
		cancel()
		return nil, func() {}, &HTTPConnectError{HTTPError{Msg: err.Error()}}
	}
	for k, v := range opts.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, func() {}, &HTTPReadTimeout{HTTPError{Msg: err.Error()}}
		}
		return nil, func() {}, &HTTPConnectError{HTTPError{Msg: err.Error()}}
	}
	cleanup := func() {
		_ = resp.Body.Close()
		cancel()
	}
	return &stdResponse{resp: resp}, cleanup, nil
}

type stdResponse struct {
	resp     *http.Response
	readBody []byte
}

func (r *stdResponse) StatusCode() int { return r.resp.StatusCode }

func (r *stdResponse) Headers() map[string]string {
	out := make(map[string]string, len(r.resp.Header))
	for k, v := range r.resp.Header {
		if len(v) > 0 {
			out[strings.ToLower(k)] = v[0]
		}
	}
	return out
}

func (r *stdResponse) ARead() ([]byte, error) {
	if r.readBody != nil {
		return r.readBody, nil
	}
	b, err := io.ReadAll(r.resp.Body)
	if err != nil {
		return nil, err
	}
	r.readBody = b
	return b, nil
}

func (r *stdResponse) Text() string { return string(r.readBody) }

func (r *stdResponse) RaiseForStatus() error {
	if r.resp.StatusCode >= 400 && r.resp.StatusCode < 600 {
		return &HTTPStatusError{StatusCode: r.resp.StatusCode}
	}
	return nil
}

func (r *stdResponse) IterLines() (<-chan string, <-chan error) {
	lines := make(chan string)
	errCh := make(chan error, 1)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(r.resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	return lines, errCh
}

// ----------------------------------------------------------------------
// PostJSON helper
// ----------------------------------------------------------------------

// PostJSON posts payload as JSON, returns the parsed JSON response.
//
// Builds Content-Type: application/json, encodes the payload once, runs the
// request, raises on HTTP errors, returns the decoded JSON.
func PostJSON(ctx context.Context, client Client, url string, payload map[string]any, headers map[string]string, timeout time.Duration) (any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	merged := map[string]string{"Content-Type": "application/json"}
	for k, v := range headers {
		merged[k] = v
	}
	resp, cleanup, err := client.StreamPost(ctx, url, StreamPostOptions{
		Headers: merged,
		Content: body,
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	defer cleanup()
	raw, err := resp.ARead()
	if err != nil {
		return nil, err
	}
	if err := resp.RaiseForStatus(); err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}
