// Package openai is a direct-HTTP LLM provider speaking the OpenAI
// chat-completions protocol.
//
// Ported from exoclaw-plugins/packages/exoclaw-provider-openai/exoclaw_provider_openai/provider.py.
//
// The provider streams the request body via HTTP/1.1 chunked transfer
// encoding, so the full JSON never materialises as one contiguous string —
// that's the peak-memory reduction the docs/memory-model.md Step B plan
// is aimed at. In Go that's just io.Pipe + a goroutine writing chunks.
//
// Per-model routing and fallback: each model name maps to exactly one
// Deployment (base URL + API key + optional extra headers), and each
// model has an optional fallback chain. A retryable error on the primary
// walks the chain; non-retryable errors (auth, 400-class) bubble up.
package openai

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	xhttp "github.com/Clause-Logic/exoclaw-go/exoclaw/http"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

const alnumChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// retryableStatus is the set of HTTP status codes that trigger the
// fallback chain. 408/425 join the 5xx/429 set for safety — some
// providers emit them on transient queue pressure.
var retryableStatus = map[int]struct{}{
	408: {}, 425: {}, 429: {},
	500: {}, 502: {}, 503: {}, 504: {},
}

// shortToolID returns a 9-char alnum tool-call id. OpenAI/Anthropic accept
// arbitrary strings for tool_calls[].id; some providers (Mistral) reject
// longer/punctuated ids, so we stay in a safe subset.
func shortToolID() string {
	var raw [9]byte
	_, _ = rand.Read(raw[:])
	var b strings.Builder
	for _, n := range raw {
		b.WriteByte(alnumChars[int(n)%len(alnumChars)])
	}
	return b.String()
}

// Deployment is a single model → endpoint binding.
//
// ExtraHeaders is merged into the request headers on every call; ExtraBody
// is merged into the JSON body at the top level (e.g. for OpenRouter's
// `provider` routing object, or a custom `transforms` flag).
type Deployment struct {
	BaseURL      string
	APIKey       string
	ExtraHeaders map[string]string
	ExtraBody    map[string]any
}

// StreamingProvider is the direct-HTTP provider speaking the OpenAI
// chat-completions protocol. Implements providers.LLMProvider.
type StreamingProvider struct {
	defaultModel       string
	deployments        map[string]Deployment
	fallbacks          map[string][]string
	requestTimeout     time.Duration
	streamTTFTTimeout  time.Duration

	client     xhttp.Client
	ownsClient bool

	llmLogging     bool
	llmLogTruncate int
	log            *slog.Logger
}

// Options bundles construction options for NewStreamingProvider.
type Options struct {
	DefaultModel      string
	Deployments       map[string]Deployment
	Fallbacks         map[string][]string
	RequestTimeout    time.Duration
	StreamTTFTTimeout time.Duration
	// Client lets tests inject a custom HTTP client. nil constructs a
	// new xhttp.Client using RequestTimeout; the provider owns and
	// closes the client in that case.
	Client xhttp.Client
	Log    *slog.Logger
}

// NewStreamingProvider constructs the provider with the supplied options.
func NewStreamingProvider(opts Options) (*StreamingProvider, error) {
	if opts.DefaultModel == "" {
		return nil, errors.New("default_model is required")
	}
	if _, ok := opts.Deployments[opts.DefaultModel]; !ok {
		keys := make([]string, 0, len(opts.Deployments))
		for k := range opts.Deployments {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		return nil, fmt.Errorf("default_model %q not in deployments: %v", opts.DefaultModel, keys)
	}
	for primary, chain := range opts.Fallbacks {
		for _, fb := range chain {
			if _, ok := opts.Deployments[fb]; !ok {
				return nil, fmt.Errorf("fallback %q (for primary %q) not in deployments", fb, primary)
			}
		}
	}

	reqTimeout := opts.RequestTimeout
	if reqTimeout == 0 {
		reqTimeout = 120 * time.Second
	}
	ttft := opts.StreamTTFTTimeout
	if ttft == 0 {
		ttft = 15 * time.Second
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	ownsClient := opts.Client == nil
	client := opts.Client
	if client == nil {
		client = xhttp.New(reqTimeout)
	}

	deployments := make(map[string]Deployment, len(opts.Deployments))
	for k, v := range opts.Deployments {
		deployments[k] = v
	}
	fallbacks := make(map[string][]string, len(opts.Fallbacks))
	for k, v := range opts.Fallbacks {
		fallbacks[k] = append([]string{}, v...)
	}

	truncate := 500
	if s := os.Getenv("LLM_LOG_TRUNCATE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			truncate = n
		}
	}

	return &StreamingProvider{
		defaultModel:      opts.DefaultModel,
		deployments:       deployments,
		fallbacks:         fallbacks,
		requestTimeout:    reqTimeout,
		streamTTFTTimeout: ttft,
		client:            client,
		ownsClient:        ownsClient,
		llmLogging:        strings.EqualFold(os.Getenv("LLM_LOGGING"), "true"),
		llmLogTruncate:    truncate,
		log:               log,
	}, nil
}

// GetDefaultModel implements providers.LLMProvider.
func (p *StreamingProvider) GetDefaultModel() string { return p.defaultModel }

// Close releases the underlying HTTP client when the provider owns it.
// Safe to call multiple times.
func (p *StreamingProvider) Close() error {
	if p.ownsClient && p.client != nil {
		err := p.client.Close()
		p.client = nil
		return err
	}
	return nil
}

// Chat implements providers.LLMProvider. Walks the fallback chain on
// retryable errors; raises the last error if every model fails.
func (p *StreamingProvider) Chat(ctx context.Context, messages []map[string]any, params providers.ChatParams) (*providers.LLMResponse, error) {
	resolved := params.Model
	if resolved == "" {
		resolved = p.defaultModel
	}
	chain := append([]string{resolved}, p.fallbacks[resolved]...)
	var lastErr error

	for i, candidate := range chain {
		deployment, ok := p.deployments[candidate]
		if !ok {
			return nil, fmt.Errorf("no deployment for model %q", candidate)
		}
		resp, err := p.chatOnce(ctx, deployment, candidate, messages, params)
		if err == nil {
			return resp, nil
		}
		var ce *providers.ContextWindowExceededError
		if errors.As(err, &ce) {
			// Context-window errors don't get better on another model
			// in the same series. Surface immediately.
			return nil, err
		}
		var re *retryableError
		if !errors.As(err, &re) {
			return nil, err
		}
		lastErr = err
		next := ""
		if i+1 < len(chain) {
			next = chain[i+1]
		}
		p.log.Warn("llm_fallback",
			"llm.model", candidate,
			"llm.next", next,
			"err", err.Error(),
		)
	}
	if lastErr == nil {
		return nil, errors.New("chat: empty fallback chain")
	}
	return nil, lastErr
}

// SendStreamingBody sends a chat-completions request with a caller-supplied
// streaming body and returns the assistant's text content.
//
// Escape hatch for callers that need full control over the request shape —
// primarily a transcription tool that streams a base64-encoded audio
// payload too big to materialise in heap. The caller builds the JSON
// envelope (model, messages, etc.) themselves; we route it to the right
// deployment, run the SSE consume, and hand back the model's text reply.
//
// No retry or fallback chain — once the body is consumed, the bytes are
// gone and can't be replayed.
func (p *StreamingProvider) SendStreamingBody(ctx context.Context, model string, body io.Reader) (string, error) {
	deployment, ok := p.deployments[model]
	if !ok {
		return "", fmt.Errorf("no deployment for model %q", model)
	}
	url := strings.TrimRight(deployment.BaseURL, "/") + "/chat/completions"
	headers := p.buildHeaders(deployment)
	resp, cleanup, err := p.client.StreamPost(ctx, url, xhttp.StreamPostOptions{
		Headers:       headers,
		ContentReader: body,
		Timeout:       p.requestTimeout,
	})
	if err != nil {
		return "", err
	}
	defer cleanup()
	if resp.StatusCode() >= 400 {
		raw, _ := resp.ARead()
		preview := raw
		if len(preview) > 500 {
			preview = preview[:500]
		}
		return "", fmt.Errorf("status %d from %s: %q", resp.StatusCode(), model, preview)
	}
	response, err := p.consumeSSEStream(ctx, resp)
	if err != nil {
		return "", err
	}
	if response.Content == nil {
		return "", nil
	}
	return *response.Content, nil
}

// chatOnce is a single non-retried request to deployment for model.
// Returns a retryableError on status/network errors the caller should
// treat as fallback-eligible. Other errors bubble up.
func (p *StreamingProvider) chatOnce(ctx context.Context, deployment Deployment, model string, messages []map[string]any, params providers.ChatParams) (*providers.LLMResponse, error) {
	url := strings.TrimRight(deployment.BaseURL, "/") + "/chat/completions"
	headers := p.buildHeaders(deployment)

	maxTokens := params.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	temperature := params.Temperature
	if temperature == 0 {
		temperature = 0.7
	}

	stream := params.Stream == nil || *params.Stream

	bodyHead := map[string]any{
		"model":       model,
		"max_tokens":  maxTokens,
		"temperature": temperature,
		"stream":      stream,
	}
	if stream {
		bodyHead["stream_options"] = map[string]any{"include_usage": true}
	}
	if len(params.Tools) > 0 {
		bodyHead["tools"] = params.Tools
		bodyHead["tool_choice"] = "auto"
	}
	if params.ReasoningEffort != "" {
		bodyHead["reasoning_effort"] = params.ReasoningEffort
	}
	if params.ResponseFormat != nil {
		bodyHead["response_format"] = encodeResponseFormat(params.ResponseFormat)
	}
	for k, v := range deployment.ExtraBody {
		if _, exists := bodyHead[k]; !exists {
			bodyHead[k] = v
		}
	}

	if p.llmLogging {
		p.logRequest(model, messages, params.Tools)
	}

	t0 := time.Now()

	bodyReader := streamBody(bodyHead, messages)
	resp, cleanup, err := p.client.StreamPost(ctx, url, xhttp.StreamPostOptions{
		Headers:       headers,
		ContentReader: bodyReader,
		Timeout:       p.requestTimeout,
	})
	if err != nil {
		var ce *xhttp.HTTPConnectError
		if errors.As(err, &ce) {
			return nil, &retryableError{Msg: fmt.Sprintf("connect error: %s", err.Error())}
		}
		var re *xhttp.HTTPReadTimeout
		if errors.As(err, &re) {
			return nil, &retryableError{Msg: fmt.Sprintf("read timeout: %s", err.Error())}
		}
		var we *xhttp.HTTPWriteTimeout
		if errors.As(err, &we) {
			return nil, &retryableError{Msg: fmt.Sprintf("write timeout: %s", err.Error())}
		}
		return nil, err
	}
	defer cleanup()

	status := resp.StatusCode()
	if _, ok := retryableStatus[status]; ok {
		raw, _ := resp.ARead()
		preview := raw
		if len(preview) > 500 {
			preview = preview[:500]
		}
		return nil, &retryableError{Msg: fmt.Sprintf("status %d: %q", status, preview)}
	}
	if status == 400 && isContextWindowError(resp) {
		return nil, &providers.ContextWindowExceededError{Message: "Prompt exceeds model context window"}
	}
	if status >= 400 {
		raw, _ := resp.ARead()
		preview := raw
		if len(preview) > 500 {
			preview = preview[:500]
		}
		return nil, fmt.Errorf("status %d: %q", status, preview)
	}

	var response *providers.LLMResponse
	if stream {
		response, err = p.consumeSSEStream(ctx, resp)
	} else {
		response, err = p.consumeOneShot(resp)
	}
	if err != nil {
		return nil, err
	}

	elapsed := time.Since(t0).Seconds()
	if p.llmLogging {
		p.logResponse(model, response, elapsed)
	}
	return response, nil
}

// consumeOneShot parses a single ChatCompletion JSON response into the
// same LLMResponse shape consumeSSEStream produces. Used when the
// request was sent with stream=false — the wire becomes a single JSON
// object instead of an SSE stream, which makes fixture-based testing
// (record once, replay forever) trivial.
//
// We reuse buildResponse so the resulting LLMResponse is structurally
// identical to the streaming path's output. The only difference is the
// "tool_calls" wire format: streaming sends per-chunk deltas under
// choices[].delta.tool_calls; one-shot sends the complete list under
// choices[].message.tool_calls.
func (p *StreamingProvider) consumeOneShot(resp xhttp.Response) (*providers.LLMResponse, error) {
	ct := strings.ToLower(resp.Headers()["content-type"])
	if ct != "" && !strings.Contains(ct, "application/json") {
		raw, _ := resp.ARead()
		preview := raw
		if len(preview) > 500 {
			preview = preview[:500]
		}
		return nil, &retryableError{Msg: fmt.Sprintf("expected application/json, got %q; body preview: %q", ct, preview)}
	}

	raw, err := resp.ARead()
	if err != nil {
		return nil, &retryableError{Msg: fmt.Sprintf("read body: %s", err.Error())}
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		return nil, fmt.Errorf("decode chat-completion body: %w", err)
	}

	var (
		contentParts   []string
		reasoningParts []string
		toolSlots      = map[int]*toolCallSlot{}
		finishReason   = "stop"
		usage          = map[string]int{}
	)

	if cu, ok := body["usage"].(map[string]any); ok && cu != nil {
		usage["prompt_tokens"] = intFromAny(cu["prompt_tokens"])
		usage["completion_tokens"] = intFromAny(cu["completion_tokens"])
		usage["total_tokens"] = intFromAny(cu["total_tokens"])
		if details, ok := cu["prompt_tokens_details"].(map[string]any); ok {
			usage["cached_tokens"] = intFromAny(details["cached_tokens"])
		}
	}

	choices, _ := body["choices"].([]any)
	for _, c := range choices {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		msg, _ := cm["message"].(map[string]any)
		if s, ok := msg["content"].(string); ok && s != "" {
			contentParts = append(contentParts, s)
		}
		if s, ok := msg["reasoning_content"].(string); ok && s != "" {
			reasoningParts = append(reasoningParts, s)
		}
		tcs, _ := msg["tool_calls"].([]any)
		for i, t := range tcs {
			tcm, ok := t.(map[string]any)
			if !ok {
				continue
			}
			slot := &toolCallSlot{}
			if id, ok := tcm["id"].(string); ok {
				slot.id = id
			}
			fn, _ := tcm["function"].(map[string]any)
			if name, ok := fn["name"].(string); ok {
				slot.name = name
			}
			if args, ok := fn["arguments"].(string); ok {
				slot.args.WriteString(args)
			}
			toolSlots[i] = slot
		}
		if fr, ok := cm["finish_reason"].(string); ok && fr != "" {
			finishReason = fr
		}
	}

	return p.buildResponse(contentParts, reasoningParts, toolSlots, finishReason, usage), nil
}

func (p *StreamingProvider) buildHeaders(deployment Deployment) map[string]string {
	headers := map[string]string{
		"Authorization": "Bearer " + deployment.APIKey,
		"Content-Type":  "application/json",
	}
	for k, v := range deployment.ExtraHeaders {
		if _, exists := headers[k]; !exists {
			headers[k] = v
		}
	}
	return headers
}

// consumeSSEStream accumulates SSE chunks into a single LLMResponse.
//
// We need the full response anyway (the turn loop wants tool_calls and
// finish_reason materialised) — streaming is purely for the server-side
// TTFT and incremental-decode wins.
//
// Implements a TTFT budget: we demand the first SSE event inside
// streamTTFTTimeout, after which the fallback chain engages.
func (p *StreamingProvider) consumeSSEStream(ctx context.Context, resp xhttp.Response) (*providers.LLMResponse, error) {
	ct := strings.ToLower(resp.Headers()["content-type"])
	if !strings.Contains(ct, "text/event-stream") {
		raw, _ := resp.ARead()
		preview := raw
		if len(preview) > 500 {
			preview = preview[:500]
		}
		return nil, &retryableError{Msg: fmt.Sprintf("expected SSE, got content-type %q; body preview: %q", ct, preview)}
	}

	var (
		contentParts   []string
		reasoningParts []string
		toolSlots      = map[int]*toolCallSlot{}
		finishReason   = "stop"
		usage          = map[string]int{}
	)

	lines, errCh := resp.IterLines()

	// TTFT budget: demand the first line within the budget.
	ttftCtx, ttftCancel := context.WithTimeout(ctx, p.streamTTFTTimeout)
	defer ttftCancel()
	var firstLine string
	var haveFirst bool
	select {
	case line, ok := <-lines:
		if !ok {
			return nil, &retryableError{Msg: "stream closed before any data"}
		}
		firstLine = line
		haveFirst = true
	case <-ttftCtx.Done():
		return nil, &retryableError{Msg: fmt.Sprintf("TTFT exceeded %s", p.streamTTFTTimeout)}
	}

	processLine := func(line string) bool {
		if line == "" {
			return false
		}
		if !strings.HasPrefix(line, "data:") {
			return false
		}
		payload := strings.TrimSpace(line[len("data:"):])
		if payload == "[DONE]" {
			return true
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return false
		}
		if cu, ok := chunk["usage"].(map[string]any); ok && cu != nil {
			usage["prompt_tokens"] = intFromAny(cu["prompt_tokens"])
			usage["completion_tokens"] = intFromAny(cu["completion_tokens"])
			usage["total_tokens"] = intFromAny(cu["total_tokens"])
			if details, ok := cu["prompt_tokens_details"].(map[string]any); ok {
				usage["cached_tokens"] = intFromAny(details["cached_tokens"])
			}
		}
		choices, _ := chunk["choices"].([]any)
		for _, c := range choices {
			cm, ok := c.(map[string]any)
			if !ok {
				continue
			}
			delta, _ := cm["delta"].(map[string]any)
			if s, ok := delta["content"].(string); ok && s != "" {
				contentParts = append(contentParts, s)
			}
			if s, ok := delta["reasoning_content"].(string); ok && s != "" {
				reasoningParts = append(reasoningParts, s)
			}
			tcs, _ := delta["tool_calls"].([]any)
			for _, t := range tcs {
				tcm, ok := t.(map[string]any)
				if !ok {
					continue
				}
				idx := intFromAny(tcm["index"])
				slot, ok := toolSlots[idx]
				if !ok {
					slot = &toolCallSlot{}
					toolSlots[idx] = slot
				}
				if id, ok := tcm["id"].(string); ok && id != "" {
					slot.id = id
				}
				fn, _ := tcm["function"].(map[string]any)
				if name, ok := fn["name"].(string); ok && name != "" {
					slot.name = name
				}
				if args, ok := fn["arguments"].(string); ok {
					slot.args.WriteString(args)
				}
			}
			if fr, ok := cm["finish_reason"].(string); ok && fr != "" {
				finishReason = fr
			}
		}
		return false
	}

	// drainAndReturn consumes any remaining lines so the IterLines
	// producer goroutine doesn't deadlock on an unbuffered send after
	// we break out on [DONE]. errCh is unconditionally drained too.
	drainAndReturn := func() (*providers.LLMResponse, error) {
		go func() {
			for range lines {
			}
		}()
		if err := <-errCh; err != nil {
			return nil, err
		}
		return p.buildResponse(contentParts, reasoningParts, toolSlots, finishReason, usage), nil
	}

	if haveFirst {
		if processLine(firstLine) {
			return drainAndReturn()
		}
	}
	for line := range lines {
		if processLine(line) {
			return drainAndReturn()
		}
	}
	if err := <-errCh; err != nil {
		return nil, err
	}
	return p.buildResponse(contentParts, reasoningParts, toolSlots, finishReason, usage), nil
}

type toolCallSlot struct {
	id   string
	name string
	args strings.Builder
}

func (p *StreamingProvider) buildResponse(contentParts, reasoningParts []string, toolSlots map[int]*toolCallSlot, finishReason string, usage map[string]int) *providers.LLMResponse {
	var content *string
	if joined := strings.Join(contentParts, ""); joined != "" {
		content = &joined
	}
	var reasoning *string
	if joined := strings.Join(reasoningParts, ""); joined != "" {
		reasoning = &joined
	}

	indices := make([]int, 0, len(toolSlots))
	for idx := range toolSlots {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	var calls []providers.ToolCallRequest
	for _, idx := range indices {
		slot := toolSlots[idx]
		if slot.name == "" {
			continue
		}
		args := map[string]any{}
		if raw := slot.args.String(); raw != "" {
			_ = json.Unmarshal([]byte(raw), &args)
		}
		id := slot.id
		if id == "" {
			id = shortToolID()
		}
		calls = append(calls, providers.ToolCallRequest{ID: id, Name: slot.name, Arguments: args})
	}

	if finishReason == "" {
		finishReason = "stop"
	}

	return &providers.LLMResponse{
		Content:          content,
		ToolCalls:        calls,
		FinishReason:     finishReason,
		Usage:            usage,
		ReasoningContent: reasoning,
	}
}

func (p *StreamingProvider) logRequest(model string, messages []map[string]any, tools []map[string]any) {
	p.log.Info("llm_request",
		"llm.model", model,
		"llm.message.count", len(messages),
		"llm.tool.count", len(tools),
	)
	for _, msg := range messages {
		role, _ := msg["role"].(string)
		if role == "" {
			role = "?"
		}
		var text string
		switch c := msg["content"].(type) {
		case string:
			text = c
		case []any:
			var parts []string
			for _, b := range c {
				if bm, ok := b.(map[string]any); ok {
					if s, ok := bm["text"].(string); ok {
						parts = append(parts, s)
					}
				}
			}
			text = strings.Join(parts, " ")
		}
		text = strings.ReplaceAll(text, "\n", "\\n")
		if p.llmLogTruncate >= 0 && len(text) > p.llmLogTruncate {
			text = text[:p.llmLogTruncate]
		}
		p.log.Info("llm_request_msg", "message.role", role, "message.text", text)
	}
}

func (p *StreamingProvider) logResponse(model string, response *providers.LLMResponse, elapsedS float64) {
	toolNames := make([]string, 0, len(response.ToolCalls))
	for _, tc := range response.ToolCalls {
		toolNames = append(toolNames, tc.Name)
	}
	p.log.Info("llm_response",
		"llm.model", model,
		"llm.token.prompt", response.Usage["prompt_tokens"],
		"llm.token.completion", response.Usage["completion_tokens"],
		"llm.token.total", response.Usage["total_tokens"],
		"llm.token.cached", response.Usage["cached_tokens"],
		"llm.duration_s", roundTo2(elapsedS),
		"llm.finish_reason", response.FinishReason,
		"llm.tools", toolNames,
	)
}

func roundTo2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}

// retryableError is the marker for errors that should trigger the
// fallback chain. Chat catches *retryableError and walks to the next
// model in the chain.
type retryableError struct {
	Msg string
}

func (e *retryableError) Error() string { return e.Msg }

// isContextWindowError is the heuristic for OpenAI's 400 + code:
// context_length_exceeded shape (OpenRouter proxies that code too).
// Only called on 400 responses so the cost of reading the body is paid
// exactly once and only in the error path.
func isContextWindowError(resp xhttp.Response) bool {
	if _, err := resp.ARead(); err != nil {
		return false
	}
	body := strings.ToLower(resp.Text())
	if body == "" {
		return false
	}
	return strings.Contains(body, "context_length_exceeded") || strings.Contains(body, "context window")
}

// ----------------------------------------------------------------------
// Streaming body builder
//
// Mirrors _stream_body / _emit_message from provider.py. In Go we use
// io.Pipe — a goroutine writes the JSON chunks to the pipe writer; the
// http.Request body reads from the pipe reader. net/http chunks the
// writes onto the wire for us.
// ----------------------------------------------------------------------

// streamBody returns an io.Reader that yields the JSON request body as
// the http client reads from it. The body is a JSON object combining
// head + a "messages" array; each message is serialised one at a time
// so the full body never lives in memory as a contiguous string.
func streamBody(head map[string]any, messages []map[string]any) io.Reader {
	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pw.Close()
		if err := writeStreamBody(pw, head, messages); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()
	return pr
}

func writeStreamBody(w io.Writer, head map[string]any, messages []map[string]any) error {
	headJSON, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(headJSON) == 0 || headJSON[len(headJSON)-1] != '}' {
		return errors.New("head_json must end with closing brace")
	}
	prefix := headJSON[:len(headJSON)-1]

	if len(prefix) > 0 && prefix[len(prefix)-1] != '{' {
		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if _, err := w.Write([]byte(`,"messages":[`)); err != nil {
			return err
		}
	} else {
		if _, err := w.Write([]byte(`{"messages":[`)); err != nil {
			return err
		}
	}

	for i, msg := range messages {
		if i > 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if err := emitMessage(w, msg); err != nil {
			return err
		}
	}
	_, err = w.Write([]byte("]}"))
	return err
}

// emitMessage serialises one message, streaming content from disk if the
// message carries a _content_file reference (Step D).
//
// Underscore-prefixed keys are stripped regardless — they're transport
// metadata, not part of the LLM message.
func emitMessage(w io.Writer, msg map[string]any) error {
	contentFile, _ := msg["_content_file"].(string)
	if contentFile == "" {
		clean := map[string]any{}
		for k, v := range msg {
			if !strings.HasPrefix(k, "_") {
				clean[k] = v
			}
		}
		body, err := json.Marshal(clean)
		if err != nil {
			return err
		}
		_, err = w.Write(body)
		return err
	}

	// File-backed: assemble {<head>, "content": "<streamed escaped>"}
	// without ever holding the full content in memory.
	head := map[string]any{}
	for k, v := range msg {
		if k != "content" && !strings.HasPrefix(k, "_") {
			head[k] = v
		}
	}
	headJSON, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(headJSON) == 0 || headJSON[len(headJSON)-1] != '}' {
		return errors.New("head_json must end with closing brace")
	}
	prefix := headJSON[:len(headJSON)-1]
	sep := []byte(",")
	if strings.TrimSpace(string(prefix)) == "{" {
		sep = nil
	}
	if _, err := w.Write(prefix); err != nil {
		return err
	}
	if _, err := w.Write(sep); err != nil {
		return err
	}
	if _, err := w.Write([]byte(`"content":"`)); err != nil {
		return err
	}

	f, err := os.Open(contentFile)
	if err != nil {
		// Scratch file disappeared between tool execution and provider
		// send (manual cleanup, OS tmpwatch, race with PostTurn). Fall
		// back to the inline content preview that the executor already
		// populated when it returned the ToolResult.
		if fallback, ok := msg["content"].(string); ok && fallback != "" {
			escaped, _ := json.Marshal(fallback)
			if len(escaped) >= 2 {
				if _, werr := w.Write(escaped[1 : len(escaped)-1]); werr != nil {
					return werr
				}
			}
		}
		_, err = w.Write([]byte(`"}`))
		return err
	}
	defer f.Close()

	buf := make([]byte, 8192)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			escaped, err := json.Marshal(string(buf[:n]))
			if err != nil {
				return err
			}
			// Strip the surrounding quotes that json.Marshal added.
			if len(escaped) >= 2 {
				if _, werr := w.Write(escaped[1 : len(escaped)-1]); werr != nil {
					return werr
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			break // fall through to writing the closing quote so the JSON stays well-formed
		}
	}
	_, err = w.Write([]byte(`"}`))
	return err
}

// encodeResponseFormat translates a providers.ResponseFormat into the
// wire-shape OpenAI expects.
func encodeResponseFormat(rf *providers.ResponseFormat) map[string]any {
	if rf == nil {
		return nil
	}
	if rf.Text != nil {
		return map[string]any{"type": "text"}
	}
	if rf.JSONObject != nil {
		return map[string]any{"type": "json_object"}
	}
	if rf.JSONSchema != nil {
		schema := map[string]any{
			"name": rf.JSONSchema.JSONSchema.Name,
		}
		if rf.JSONSchema.JSONSchema.Description != "" {
			schema["description"] = rf.JSONSchema.JSONSchema.Description
		}
		if rf.JSONSchema.JSONSchema.Schema != nil {
			schema["schema"] = rf.JSONSchema.JSONSchema.Schema
		}
		if rf.JSONSchema.JSONSchema.Strict != nil {
			schema["strict"] = *rf.JSONSchema.JSONSchema.Strict
		}
		return map[string]any{
			"type":        "json_schema",
			"json_schema": schema,
		}
	}
	return nil
}

func intFromAny(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	}
	return 0
}

// Compile-time check that StreamingProvider satisfies the core interface.
var _ providers.LLMProvider = (*StreamingProvider)(nil)
