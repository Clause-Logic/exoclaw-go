package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
	"github.com/standd/exoclaw-go/exoclaw/providers"
)

// Ported from exoclaw_tools_workspace/web.py.

// UserAgent is the User-Agent header used by WebFetchTool. Mirrors the
// Python original.
const UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) AppleWebKit/537.36"

// MaxRedirects caps redirect chains to prevent DoS.
const MaxRedirects = 5

// Readability is the pluggable HTML → text/markdown extractor. The Python
// original uses `readability-lxml`; we ship a stdlib-only fallback that
// strips all tags + normalises whitespace. Downstream callers that want
// real article-body extraction can swap in github.com/go-shiori/go-readability
// at the call site without touching this package.
//
// Returns (title, content). title may be "".
type Readability func(htmlBody, mode string) (string, string)

// DefaultReadability is the stdlib-only extractor: strips tags and
// normalises whitespace. Title comes from <title>…</title>.
func DefaultReadability(htmlBody, mode string) (string, string) {
	title := extractTitle(htmlBody)
	if mode == "markdown" {
		return title, htmlToMarkdown(htmlBody)
	}
	return title, normalizeWS(stripTags(htmlBody))
}

var (
	titleRE  = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	scriptRE = regexp.MustCompile(`(?is)<script[\s\S]*?</script>`)
	styleRE  = regexp.MustCompile(`(?is)<style[\s\S]*?</style>`)
	tagRE    = regexp.MustCompile(`<[^>]+>`)
	wsTabRE  = regexp.MustCompile(`[ \t]+`)
	wsNlRE   = regexp.MustCompile(`\n{3,}`)
)

func extractTitle(htmlBody string) string {
	m := titleRE.FindStringSubmatch(htmlBody)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(html.UnescapeString(stripTags(m[1])))
}

func stripTags(text string) string {
	text = scriptRE.ReplaceAllString(text, "")
	text = styleRE.ReplaceAllString(text, "")
	text = tagRE.ReplaceAllString(text, "")
	return strings.TrimSpace(html.UnescapeString(text))
}

func normalizeWS(text string) string {
	text = wsTabRE.ReplaceAllString(text, " ")
	text = wsNlRE.ReplaceAllString(text, "\n\n")
	return strings.TrimSpace(text)
}

func htmlToMarkdown(htmlBody string) string {
	// Anchor → [text](url)
	text := regexp.MustCompile(`(?is)<a\s+[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)</a>`).
		ReplaceAllStringFunc(htmlBody, func(s string) string {
			m := regexp.MustCompile(`(?is)<a\s+[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)</a>`).FindStringSubmatch(s)
			return fmt.Sprintf("[%s](%s)", stripTags(m[2]), m[1])
		})
	// Headings.
	text = regexp.MustCompile(`(?is)<h([1-6])[^>]*>([\s\S]*?)</h[1-6]>`).
		ReplaceAllStringFunc(text, func(s string) string {
			m := regexp.MustCompile(`(?is)<h([1-6])[^>]*>([\s\S]*?)</h[1-6]>`).FindStringSubmatch(s)
			level := 1
			fmt.Sscanf(m[1], "%d", &level)
			return fmt.Sprintf("\n%s %s\n", strings.Repeat("#", level), stripTags(m[2]))
		})
	// List items.
	text = regexp.MustCompile(`(?is)<li[^>]*>([\s\S]*?)</li>`).
		ReplaceAllStringFunc(text, func(s string) string {
			m := regexp.MustCompile(`(?is)<li[^>]*>([\s\S]*?)</li>`).FindStringSubmatch(s)
			return "\n- " + stripTags(m[1])
		})
	// Block closings.
	text = regexp.MustCompile(`(?is)</(p|div|section|article)>`).ReplaceAllString(text, "\n\n")
	text = regexp.MustCompile(`(?is)<(br|hr)\s*/?>`).ReplaceAllString(text, "\n")
	return normalizeWS(stripTags(text))
}

func validateURL(raw string) (bool, string) {
	p, err := url.Parse(raw)
	if err != nil {
		return false, err.Error()
	}
	if p.Scheme != "http" && p.Scheme != "https" {
		scheme := p.Scheme
		if scheme == "" {
			scheme = "none"
		}
		return false, fmt.Sprintf("Only http/https allowed, got '%s'", scheme)
	}
	if p.Host == "" {
		return false, "Missing domain"
	}
	return true, ""
}

// ----------------------------------------------------------------------
// WebSearchTool
// ----------------------------------------------------------------------

// WebSearchTool searches the web via the Brave Search API or a search
// model (provider.Chat). The provider mode wins when both are configured.
type WebSearchTool struct {
	tools.ToolBase

	initAPIKey  string
	MaxResults  int
	Proxy       string
	Provider    providers.LLMProvider
	SearchModel string
	HTTPClient  *http.Client
}

// WebSearchOptions bundles construction options.
type WebSearchOptions struct {
	APIKey      string
	MaxResults  int
	Proxy       string
	Provider    providers.LLMProvider
	SearchModel string
	HTTPClient  *http.Client
}

// NewWebSearchTool constructs a WebSearchTool. APIKey defaults to the
// BRAVE_API_KEY environment variable at call time when unset.
func NewWebSearchTool(opts WebSearchOptions) *WebSearchTool {
	max := opts.MaxResults
	if max <= 0 {
		max = 5
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	t := &WebSearchTool{
		initAPIKey:  opts.APIKey,
		MaxResults:  max,
		Proxy:       opts.Proxy,
		Provider:    opts.Provider,
		SearchModel: opts.SearchModel,
		HTTPClient:  httpClient,
	}
	t.NameField = "web_search"
	t.DescriptionField = "Search the web. Returns titles, URLs, and snippets."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "description": "Search query"},
			"count": map[string]any{
				"type":        "integer",
				"description": "Results (1-10)",
				"minimum":     1,
				"maximum":     10,
			},
		},
		"required": []any{"query"},
	}
	return t
}

// APIKey resolves the Brave API key at call time so env / config changes
// during the process lifetime are picked up.
func (t *WebSearchTool) APIKey() string {
	if t.initAPIKey != "" {
		return t.initAPIKey
	}
	return os.Getenv("BRAVE_API_KEY")
}

// Execute runs the search. Prefers the provider-driven model search when
// configured; falls back to the Brave Search API.
func (t *WebSearchTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	query, _ := params["query"].(string)
	count, _ := intOptional(params["count"])

	if t.SearchModel != "" && t.Provider != nil {
		return t.searchViaModel(ctx, query, count)
	}
	return t.searchViaBrave(ctx, query, count)
}

// ExecuteWithContext mirrors Execute but lets the model search route the
// LLM call through the executor (durable retries, etc.) when one is
// available.
func (t *WebSearchTool) ExecuteWithContext(ctx context.Context, _ *tools.ToolContext, params map[string]any) (string, error) {
	// In Go the executor escape hatch isn't strongly typed at this layer;
	// the call falls back to provider.Chat. Durable executors that want
	// to intercept can wrap WebSearchTool with their own adapter.
	return t.Execute(ctx, params)
}

func (t *WebSearchTool) searchViaModel(ctx context.Context, query string, count int) (string, error) {
	n := count
	if n <= 0 {
		n = t.MaxResults
	}
	if n < 1 {
		n = 1
	} else if n > 10 {
		n = 10
	}
	resp, err := t.Provider.Chat(ctx, []map[string]any{
		{"role": "user", "content": fmt.Sprintf(
			"Search the web for: %s\n\nReturn the top %d results with titles, URLs, and brief descriptions.",
			query, n,
		)},
	}, providers.ChatParams{
		Model:       t.SearchModel,
		MaxTokens:   1024,
		Temperature: 0.1,
	})
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if resp.Content == nil || *resp.Content == "" {
		return "No results for: " + query, nil
	}
	return *resp.Content, nil
}

type braveSearchResponse struct {
	Web struct {
		Results []struct {
			Title       string `json:"title"`
			URL         string `json:"url"`
			Description string `json:"description"`
		} `json:"results"`
	} `json:"web"`
}

func (t *WebSearchTool) searchViaBrave(ctx context.Context, query string, count int) (string, error) {
	if t.APIKey() == "" {
		return "Error: Brave Search API key not configured. Set BRAVE_API_KEY environment variable.", nil
	}
	n := count
	if n <= 0 {
		n = t.MaxResults
	}
	if n < 1 {
		n = 1
	} else if n > 10 {
		n = 10
	}
	u := fmt.Sprintf("https://api.search.brave.com/res/v1/web/search?q=%s&count=%d",
		url.QueryEscape(query), n)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Subscription-Token", t.APIKey())

	client := t.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if t.Proxy != "" {
		proxyURL, perr := url.Parse(t.Proxy)
		if perr != nil {
			return "Error: " + perr.Error(), nil
		}
		// Clone with proxy applied — don't mutate t.HTTPClient.
		transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		client = &http.Client{Timeout: client.Timeout, Transport: transport}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Sprintf("Error: HTTP %d from Brave Search", resp.StatusCode), nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	var parsed braveSearchResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "Error: " + err.Error(), nil
	}
	if len(parsed.Web.Results) == 0 {
		return "No results for: " + query, nil
	}
	if len(parsed.Web.Results) > n {
		parsed.Web.Results = parsed.Web.Results[:n]
	}
	lines := []string{"Results for: " + query, ""}
	for i, r := range parsed.Web.Results {
		lines = append(lines, fmt.Sprintf("%d. %s\n   %s", i+1, r.Title, r.URL))
		if r.Description != "" {
			lines = append(lines, "   "+r.Description)
		}
	}
	return strings.Join(lines, "\n"), nil
}

var (
	_ tools.Tool           = (*WebSearchTool)(nil)
	_ tools.ContextualTool = (*WebSearchTool)(nil)
)

// ----------------------------------------------------------------------
// WebFetchTool
// ----------------------------------------------------------------------

// WebFetchTool fetches a URL and extracts readable content. The result is
// JSON-encoded: {url, finalUrl, status, extractor, truncated, length, text}.
type WebFetchTool struct {
	tools.ToolBase

	MaxChars    int
	Proxy       string
	HTTPClient  *http.Client
	Readability Readability
}

// WebFetchOptions bundles construction options.
type WebFetchOptions struct {
	MaxChars    int
	Proxy       string
	HTTPClient  *http.Client
	Readability Readability // defaults to DefaultReadability when nil
}

// NewWebFetchTool constructs a WebFetchTool.
func NewWebFetchTool(opts WebFetchOptions) *WebFetchTool {
	mc := opts.MaxChars
	if mc <= 0 {
		mc = 50_000
	}
	rd := opts.Readability
	if rd == nil {
		rd = DefaultReadability
	}
	t := &WebFetchTool{
		MaxChars:    mc,
		Proxy:       opts.Proxy,
		HTTPClient:  opts.HTTPClient,
		Readability: rd,
	}
	t.NameField = "web_fetch"
	t.DescriptionField = "Fetch URL and extract readable content (HTML → markdown/text)."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"url":         map[string]any{"type": "string", "description": "URL to fetch"},
			"extractMode": map[string]any{"type": "string", "enum": []any{"markdown", "text"}, "default": "markdown"},
			"maxChars":    map[string]any{"type": "integer", "minimum": 100},
		},
		"required": []any{"url"},
	}
	return t
}

// Execute fetches the URL and returns the JSON-encoded result envelope.
func (t *WebFetchTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	rawURL, _ := params["url"].(string)
	mode, _ := params["extractMode"].(string)
	if mode == "" {
		mode = "markdown"
	}
	maxChars, _ := intOptional(params["maxChars"])
	if maxChars <= 0 {
		maxChars = t.MaxChars
	}

	if ok, msg := validateURL(rawURL); !ok {
		return jsonEnvelope(map[string]any{
			"error": "URL validation failed: " + msg,
			"url":   rawURL,
		}), nil
	}

	client := t.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= MaxRedirects {
					return fmt.Errorf("stopped after %d redirects", MaxRedirects)
				}
				return nil
			},
		}
	}
	if t.Proxy != "" {
		proxyURL, err := url.Parse(t.Proxy)
		if err != nil {
			return jsonEnvelope(map[string]any{"error": "Proxy error: " + err.Error(), "url": rawURL}), nil
		}
		client = &http.Client{
			Timeout: client.Timeout,
			Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= MaxRedirects {
					return fmt.Errorf("stopped after %d redirects", MaxRedirects)
				}
				return nil
			},
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return jsonEnvelope(map[string]any{"error": err.Error(), "url": rawURL}), nil
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return jsonEnvelope(map[string]any{"error": err.Error(), "url": rawURL}), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return jsonEnvelope(map[string]any{
			"error": fmt.Sprintf("HTTP %d", resp.StatusCode),
			"url":   rawURL,
		}), nil
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return jsonEnvelope(map[string]any{"error": err.Error(), "url": rawURL}), nil
	}
	body := string(bodyBytes)

	ctype := resp.Header.Get("content-type")
	extractor := "raw"
	text := body
	switch {
	case strings.Contains(ctype, "application/json"):
		var parsed any
		if err := json.Unmarshal(bodyBytes, &parsed); err == nil {
			if pretty, err := json.MarshalIndent(parsed, "", "  "); err == nil {
				text = string(pretty)
			}
		}
		extractor = "json"
	case strings.Contains(ctype, "text/html") || looksLikeHTML(body):
		title, content := t.Readability(body, mode)
		if title != "" {
			text = "# " + title + "\n\n" + content
		} else {
			text = content
		}
		extractor = "readability"
	}

	truncated := len(text) > maxChars
	if truncated {
		text = text[:maxChars]
	}
	return jsonEnvelope(map[string]any{
		"url":       rawURL,
		"finalUrl":  resp.Request.URL.String(),
		"status":    resp.StatusCode,
		"extractor": extractor,
		"truncated": truncated,
		"length":    len(text),
		"text":      text,
	}), nil
}

func looksLikeHTML(s string) bool {
	prefix := s
	if len(prefix) > 256 {
		prefix = prefix[:256]
	}
	lower := strings.ToLower(strings.TrimLeft(prefix, " \t\r\n"))
	return strings.HasPrefix(lower, "<!doctype") || strings.HasPrefix(lower, "<html")
}

func jsonEnvelope(m map[string]any) string {
	b, err := json.Marshal(m)
	if err != nil {
		return `{"error":"marshal envelope"}`
	}
	return string(b)
}

var _ tools.Tool = (*WebFetchTool)(nil)
