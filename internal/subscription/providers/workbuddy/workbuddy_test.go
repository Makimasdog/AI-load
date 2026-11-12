package workbuddy

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseCredentialJSONShapes(t *testing.T) {
	canonical := `{"type":"workbuddy","realm":"global","base_url":"https://www.workbuddy.ai","access_token":"at","refresh_token":"rt","expires_at":1790000000,"uid":"u1"}`
	value, err := ParseCredentialJSON([]byte(canonical))
	if err != nil {
		t.Fatalf("canonical parse: %v", err)
	}
	if value.UID != "u1" || value.Realm != realmGlobal || value.BaseURL != defaultGlobalBaseURL {
		t.Fatalf("canonical value = %#v", value)
	}

	nested := `{"auth":{"accessToken":"at2","refreshToken":"rt2","expiresAt":1790000000000,"domain":"www.workbuddy.ai"},"account":{"uid":"u2","enterpriseId":"e2","nickname":"n2"}}`
	value, err = ParseCredentialJSON([]byte(nested))
	if err != nil {
		t.Fatalf("nested parse: %v", err)
	}
	if value.UID != "u2" || value.EnterpriseID != "e2" || value.ExpiresAt != 1790000000 {
		t.Fatalf("nested value = %#v", value)
	}

	raw := `{"code":0,"data":{"accessToken":"at3","refreshToken":"rt3","expiresIn":5184000,"domain":"copilot.tencent.com","uid":"u3"}}`
	value, err = ParseCredentialJSON([]byte(raw))
	if err != nil {
		t.Fatalf("raw parse: %v", err)
	}
	if value.Realm != realmCN || value.BaseURL != defaultCNBaseURL || value.UID != "u3" {
		t.Fatalf("raw value = %#v", value)
	}

	if _, err := ParseCredentialJSON([]byte(`{"$wbEncrypted":"cipher"}`)); !errors.Is(err, ErrCredentialEncrypted) {
		t.Fatalf("encrypted error = %v", err)
	}
	if _, err := ParseCredentialJSON([]byte(`{"access_token":"at"}`)); !errors.Is(err, ErrCredentialIncomplete) {
		t.Fatalf("incomplete error = %v", err)
	}
}

func TestNormalizeRequestBody(t *testing.T) {
	raw := `{
		"model":"gpt-5","messages":[
			{"role":"developer","content":"rules"},
			{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":"https://example.com/a.png"},{"type":"reasoning_text","text":"drop"}]},
			{"role":"assistant","content":"ok","tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"done"},
			{"role":"tool","tool_call_id":"missing","content":"drop"}
		],
		"max_completion_tokens":8,
		"tool_choice":{"type":"none"},
		"store":true
	}`
	payload, err := normalizeRequestBody([]byte(raw), "gpt-5", Credential{UID: "u1"}, conversationIDs{Conversation: "conv"})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["stream"] != true {
		t.Fatalf("stream = %v", body["stream"])
	}
	if options, ok := body["stream_options"].(map[string]any); !ok || options["include_usage"] != true {
		t.Fatalf("stream_options = %v", body["stream_options"])
	}
	if _, ok := body["store"]; ok {
		t.Fatal("store must be dropped")
	}
	if _, ok := body["tool_choice"]; ok {
		t.Fatal("tool_choice none must be dropped")
	}
	if _, ok := body["tools"]; ok {
		t.Fatal("tools must be dropped alongside tool_choice none")
	}
	messages := body["messages"].([]any)
	if len(messages) != 4 {
		t.Fatalf("messages = %d, want 4", len(messages))
	}
	if role := messages[0].(map[string]any)["role"]; role != "system" {
		t.Fatalf("first role = %v", role)
	}
	if body["max_tokens"].(float64) != 16 {
		t.Fatalf("max_tokens = %v", body["max_tokens"])
	}
	if body["reasoning_effort"] != "high" {
		t.Fatalf("reasoning_effort = %v", body["reasoning_effort"])
	}
	if key, _ := body["prompt_cache_key"].(string); !strings.HasPrefix(key, "wb2a-") || !strings.HasSuffix(key, "-conv") {
		t.Fatalf("prompt_cache_key = %v", key)
	}
	parts := messages[1].(map[string]any)["content"].([]any)
	if len(parts) != 2 {
		t.Fatalf("content parts = %d, want 2", len(parts))
	}
	image, _ := parts[1].(map[string]any)["image_url"].(map[string]any)
	if image["url"] != "https://example.com/a.png" {
		t.Fatalf("image_url = %v", parts[1])
	}
}

func TestNormalizeRequestBodyRewritesBlockedPromptFingerprint(t *testing.T) {
	raw := `{"model":"hy3","messages":[
		{"role":"system","content":[{"type":"text","text":"Current branch: main\nMain branch (you will usually use this for PRs): main"}]},
		{"role":"user","content":"Main branch (you will usually use this for PRs): dev"}
	]}`
	payload, err := normalizeRequestBody([]byte(raw), "hy3", Credential{UID: "u1"}, conversationIDs{Conversation: "conv"})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	messages := body["messages"].([]any)
	systemPart := messages[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	if strings.Contains(strings.ToLower(systemPart), "you will usually use this for prs") {
		t.Fatalf("system fingerprint remained: %q", systemPart)
	}
	if !strings.Contains(systemPart, "Main branch (usually the PR target): main") {
		t.Fatalf("system rewrite lost meaning: %q", systemPart)
	}
	userText := messages[1].(map[string]any)["content"].(string)
	if strings.Contains(strings.ToLower(userText), "you will usually use this for prs") {
		t.Fatalf("user fingerprint remained: %q", userText)
	}
}

func chunkJSON(delta string) []byte {
	return []byte(`{"id":"c1","model":"hy3","choices":[{"index":0,"delta":` + delta + `,"logprobs":null,"finish_reason":""}],"usage":null}`)
}

// reasoningDeltaJSON 还原上游 reasoning chunk 的完整占位字段。
func reasoningDeltaJSON(text string) []byte {
	return chunkJSON(`{"content":"","reasoning_content":"` + text + `","function_call":null,"refusal":"","tool_calls":[],"extra_fields":null}`)
}

func TestReasoningCoalescerMergesTokenDeltas(t *testing.T) {
	var emitted []string
	coalescer := newReasoningCoalescer(func(payload []byte) bool {
		var chunk map[string]any
		if err := json.Unmarshal(payload, &chunk); err != nil {
			t.Fatalf("decode emitted: %v", err)
		}
		choices := chunk["choices"].([]any)
		delta := choices[0].(map[string]any)["delta"].(map[string]any)
		if text, ok := delta["reasoning_content"].(string); ok && text != "" {
			emitted = append(emitted, text)
			return true
		}
		emitted = append(emitted, "CONTENT:"+delta["content"].(string))
		return true
	})
	tokens := []string{"We", " need", " answer", " three", " short", " steps", " quickly", " today"}
	for _, token := range tokens {
		if !coalescer.Push(reasoningDeltaJSON(token)) {
			t.Fatal("push rejected")
		}
	}
	coalescer.Push(chunkJSON(`{"content":"done"}`))
	coalescer.Close()

	if len(emitted) < 2 {
		t.Fatalf("emitted = %v, want merged reasoning", emitted)
	}
	if emitted[0] != "We" {
		t.Fatalf("first delta = %q, want single token without added latency", emitted[0])
	}
	merged := strings.Join(emitted[1:len(emitted)-1], "")
	if merged != strings.Join(tokens[1:], "") {
		t.Fatalf("merged reasoning = %q", merged)
	}
	for _, part := range emitted[1 : len(emitted)-1] {
		if utf8.RuneCountInString(part) < 8 {
			t.Fatalf("reasoning chunk too small: %q", part)
		}
	}
	if emitted[len(emitted)-1] != "CONTENT:done" {
		t.Fatalf("content chunk = %q", emitted[len(emitted)-1])
	}
}

func TestNormalizeStreamChunkDropsEmptyPlaceholderFields(t *testing.T) {
	normalized, err := normalizeStreamChunk(chunkJSON(
		`{"content":"","reasoning_content":"thinking","function_call":null,"refusal":"","tool_calls":[],"extra_fields":null}`,
	))
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var chunk map[string]any
	if err := json.Unmarshal(normalized, &chunk); err != nil {
		t.Fatalf("decode: %v", err)
	}
	delta := chunk["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	for _, key := range []string{"tool_calls", "function_call", "refusal", "extra_fields"} {
		if _, exists := delta[key]; exists {
			t.Fatalf("empty placeholder %q survived: %v", key, delta)
		}
	}
	if delta["reasoning_content"] != "thinking" || delta["content"] != "" {
		t.Fatalf("delta content changed: %v", delta)
	}

	normalized, err = normalizeStreamChunk(chunkJSON(
		`{"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`,
	))
	if err != nil {
		t.Fatalf("normalize tool call: %v", err)
	}
	var withCalls map[string]any
	if err := json.Unmarshal(normalized, &withCalls); err != nil {
		t.Fatalf("decode tool call: %v", err)
	}
	calls := withCalls["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"]
	if list, ok := calls.([]any); !ok || len(list) != 1 {
		t.Fatalf("non-empty tool_calls dropped: %v", calls)
	}

	normalized, err = normalizeStreamChunk(chunkJSON(`{"function_call":{"arguments":"","name":""}}`))
	if err != nil {
		t.Fatalf("normalize empty function call: %v", err)
	}
	var emptyCall map[string]any
	if err := json.Unmarshal(normalized, &emptyCall); err != nil {
		t.Fatalf("decode empty function call: %v", err)
	}
	emptyDelta := emptyCall["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if _, exists := emptyDelta["function_call"]; exists {
		t.Fatalf("empty function_call survived: %v", emptyDelta)
	}

	normalized, err = normalizeStreamChunk(chunkJSON(`{"function_call":{"arguments":"{}","name":"lookup"}}`))
	if err != nil {
		t.Fatalf("normalize function call: %v", err)
	}
	var keptCall map[string]any
	if err := json.Unmarshal(normalized, &keptCall); err != nil {
		t.Fatalf("decode function call: %v", err)
	}
	keptDelta := keptCall["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if _, exists := keptDelta["function_call"]; !exists {
		t.Fatalf("non-empty function_call dropped: %v", keptDelta)
	}
}

func TestClassifyHTTPError(t *testing.T) {
	cases := []struct {
		status     int
		body       string
		wantStatus int
		wantCode   string
		credential bool
		request    bool
		model      bool
	}{
		{http.StatusUnauthorized, `{"code":12153,"msg":"Offline user session not found"}`, 401, "12153", true, false, false},
		{http.StatusForbidden, `<html>waf</html>`, 403, "", false, false, false},
		{http.StatusForbidden, `{"code":11140,"msg":"account restricted"}`, 403, "11140", true, false, false},
		{http.StatusBadRequest, `{"error":{"code":11101,"message":"invalid request"}}`, 400, "11101", false, true, false},
		{http.StatusTooManyRequests, `{"code":"14018","msg":"Credits exhausted"}`, 429, "14018", false, false, true},
		{http.StatusOK, `{"code":6004,"msg":"model rate-limiting until 2026-11-11 12:00:00"}`, 429, "6004", false, false, true},
		{http.StatusOK, `{"code":"11102","msg":"unknown model"}`, 400, "11102", false, false, true},
	}
	for _, test := range cases {
		err := classifyHTTPError(test.status, []byte(test.body), nil)
		if err == nil {
			t.Fatalf("status %d: expected error", test.status)
		}
		if err.Status != test.wantStatus || err.ErrorCode() != test.wantCode {
			t.Fatalf("status %d: got status=%d code=%s", test.status, err.Status, err.ErrorCode())
		}
		if err.Credential != test.credential || err.Request != test.request || err.Model != test.model {
			t.Fatalf("status %d: scopes = credential:%t request:%t model:%t", test.status, err.Credential, err.Request, err.Model)
		}
	}
}

func TestAggregatorBuildsCompletion(t *testing.T) {
	aggregate := newAggregator("gpt-5")
	chunks := []string{
		`{"id":"c1","created":1,"model":"gpt-5","choices":[{"index":0,"delta":{"content":"he"}}]}`,
		`{"id":"c1","choices":[{"index":0,"delta":{"content":"llo","reasoning_content":"r"}}]}`,
		`{"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"a\":"}}]}}]}`,
		`{"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]}`,
		`{"id":"c1","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3,"prompt_cache_hit_tokens":1}}`,
	}
	for _, chunk := range chunks {
		normalized, err := normalizeStreamChunk([]byte(chunk))
		if err != nil {
			t.Fatalf("normalize chunk: %v", err)
		}
		if err := aggregate.consume(normalized); err != nil {
			t.Fatalf("consume: %v", err)
		}
	}
	payload, err := aggregate.finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	var completion struct {
		Choices []struct {
			Message      map[string]any `json:"message"`
			FinishReason string         `json:"finish_reason"`
		} `json:"choices"`
		Usage map[string]any `json:"usage"`
	}
	if err := json.Unmarshal(payload, &completion); err != nil {
		t.Fatalf("decode completion: %v", err)
	}
	if completion.Choices[0].Message["content"] != "hello" || completion.Choices[0].Message["reasoning_content"] != "r" {
		t.Fatalf("message = %#v", completion.Choices[0].Message)
	}
	if completion.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("finish_reason = %s", completion.Choices[0].FinishReason)
	}
	details := completion.Usage["prompt_tokens_details"].(map[string]any)
	if details["cached_tokens"].(float64) != 1 {
		t.Fatalf("usage = %#v", completion.Usage)
	}
	calls := completion.Choices[0].Message["tool_calls"].([]any)
	function := calls[0].(map[string]any)["function"].(map[string]any)
	if function["name"] != "f" || function["arguments"] != `{"a":1}` {
		t.Fatalf("tool call = %#v", calls[0])
	}
}
