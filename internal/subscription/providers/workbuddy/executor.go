package workbuddy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const maxChatResponseBytes = 1 << 20

// ExecuteRequest mirrors the provider-neutral execution request contract.
type ExecuteRequest struct {
	AttemptID            string
	Model                string
	Payload              []byte
	Format               string
	Headers              http.Header
	OriginalRequest      []byte
	ContinuityKey        string
	BaseURL              string
	ProxyURL             string
	ProxyFromEnvironment bool
	ConfiguredHeaders    []string
}

// ExecuteResponse is a complete (aggregated) upstream response.
type ExecuteResponse struct {
	Payload []byte
	Headers http.Header
}

// StreamChunk is one unframed OpenAI chunk produced by the upstream stream.
type StreamChunk struct {
	Payload    []byte
	Incomplete bool
	Err        error
}

// StreamResponse is an open upstream SSE stream.
type StreamResponse struct {
	Headers http.Header
	Chunks  <-chan StreamChunk
}

// Executor performs WorkBuddy chat completions.
type Executor interface {
	Execute(context.Context, string, Credential, ExecuteRequest) (ExecuteResponse, error)
	ExecuteStream(context.Context, string, Credential, ExecuteRequest) (*StreamResponse, error)
}

type upstreamExecutor struct{}

func NewExecutor() Executor { return &upstreamExecutor{} }

// UpstreamError carries the bounded classification of a failed upstream call.
type UpstreamError struct {
	Status     int
	Code       string
	Message    string
	Request    bool
	Credential bool
	Model      bool
	retry      time.Duration
}

func (err *UpstreamError) Error() string {
	if err == nil {
		return ""
	}
	code := normalizeErrorCode(err.Code)
	switch {
	case err.Status != 0 && code != "":
		return fmt.Sprintf("WorkBuddy upstream rejected the request (status=%d code=%s)", err.Status, code)
	case err.Status != 0:
		return fmt.Sprintf("WorkBuddy upstream rejected the request (status=%d)", err.Status)
	case code != "":
		return fmt.Sprintf("WorkBuddy upstream rejected the request (code=%s)", code)
	default:
		return "WorkBuddy upstream rejected the request"
	}
}

func (err *UpstreamError) StatusCode() int   { return err.Status }
func (err *UpstreamError) ErrorCode() string { return normalizeErrorCode(err.Code) }

func (err *UpstreamError) RetryAfter() *time.Duration {
	if err == nil || err.retry <= 0 {
		return nil
	}
	value := err.retry
	return &value
}

func (err *UpstreamError) IsRequestScoped() bool    { return err != nil && err.Request }
func (err *UpstreamError) IsCredentialScoped() bool { return err != nil && err.Credential }
func (err *UpstreamError) IsModelScoped() bool      { return err != nil && err.Model }

func (executor *upstreamExecutor) Execute(ctx context.Context, credentialID string, credential Credential, request ExecuteRequest) (ExecuteResponse, error) {
	response, err := executor.open(ctx, credential, request)
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer response.Body.Close()
	if !isEventStream(response.Header) {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxChatResponseBytes))
		if readErr != nil {
			return ExecuteResponse{}, readErr
		}
		if err := businessError(body); err != nil {
			return ExecuteResponse{}, err
		}
		if !looksLikeCompletion(body) {
			return ExecuteResponse{}, &UpstreamError{Status: http.StatusBadGateway, Code: "unexpected_response", Message: "WorkBuddy returned a non-JSON response"}
		}
		return ExecuteResponse{Payload: body, Headers: sanitizedHeaders(response.Header)}, nil
	}
	aggregate := newAggregator(request.Model)
	err = consumeSSE(response.Body, func(payload []byte) error {
		normalized, normalizeErr := normalizeStreamChunk(payload)
		if normalizeErr != nil {
			return nil
		}
		if err := businessError(normalized); err != nil {
			return err
		}
		return aggregate.consume(normalized)
	})
	if err != nil && !errors.Is(err, errStreamDone) {
		return ExecuteResponse{}, err
	}
	if !aggregate.saw {
		return ExecuteResponse{}, emptyStreamError()
	}
	payload, err := aggregate.finish()
	if err != nil {
		return ExecuteResponse{}, err
	}
	return ExecuteResponse{Payload: payload, Headers: sanitizedHeaders(response.Header)}, nil
}

func (executor *upstreamExecutor) ExecuteStream(ctx context.Context, credentialID string, credential Credential, request ExecuteRequest) (*StreamResponse, error) {
	response, err := executor.open(ctx, credential, request)
	if err != nil {
		return nil, err
	}
	chunks := make(chan StreamChunk)
	go func() {
		defer close(chunks)
		defer response.Body.Close()
		send := func(chunk StreamChunk) bool {
			select {
			case chunks <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !isEventStream(response.Header) {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, maxChatResponseBytes))
			if readErr != nil {
				send(StreamChunk{Err: readErr})
				return
			}
			if err := businessError(body); err != nil {
				send(StreamChunk{Err: err})
				return
			}
			normalized, normalizeErr := normalizeStreamChunk(body)
			if normalizeErr != nil {
				send(StreamChunk{Err: emptyStreamError()})
				return
			}
			send(StreamChunk{Payload: normalized})
			return
		}
		saw := false
		// 上游逐 token 下发 reasoning，客户端思考面板会一词一行；
		// 这里合并成较大的片段再转发，正文 content 不受影响。
		coalescer := newReasoningCoalescer(func(payload []byte) bool {
			return send(StreamChunk{Payload: payload})
		})
		streamErr := consumeSSE(response.Body, func(payload []byte) error {
			normalized, normalizeErr := normalizeStreamChunk(payload)
			if normalizeErr != nil {
				return nil
			}
			if err := businessError(normalized); err != nil {
				return err
			}
			saw = true
			if !coalescer.Push(normalized) {
				return context.Canceled
			}
			return nil
		})
		coalescer.Close()
		switch {
		case errors.Is(streamErr, errStreamDone):
			if !saw {
				send(StreamChunk{Err: emptyStreamError()})
			}
		case streamErr != nil && !errors.Is(streamErr, context.Canceled):
			send(StreamChunk{Err: streamErr})
		case !saw:
			send(StreamChunk{Err: emptyStreamError()})
		}
	}()
	return &StreamResponse{Headers: sanitizedHeaders(response.Header), Chunks: chunks}, nil
}

func (executor *upstreamExecutor) open(ctx context.Context, credential Credential, request ExecuteRequest) (*http.Response, error) {
	if format := strings.TrimSpace(request.Format); format != "" && format != "openai" {
		return nil, fmt.Errorf("WorkBuddy only supports OpenAI chat completions, got %q", format)
	}
	base := normalizeBaseURL(firstNonEmpty(request.BaseURL, credential.BaseURL), realmFor(credential.Realm, request.BaseURL, credential.Domain))
	realm := realmFor("", base, credential.Domain)
	conversation := deriveConversationIDs(request, credential)
	payload, err := normalizeRequestBody(request.Payload, request.Model, credential, conversation)
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v2/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	applyHeaders(httpRequest.Header, chatHeaders(credential, realm, conversation))
	client, err := clientForRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, maxChatResponseBytes))
		closeErr := response.Body.Close()
		_ = closeErr
		return nil, classifyHTTPError(response.StatusCode, body, response.Header)
	}
	return response, nil
}

func clientForRequest(ctx context.Context, request ExecuteRequest) (*http.Client, error) {
	if strings.TrimSpace(request.ProxyURL) != "" || request.ProxyFromEnvironment {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		switch {
		case strings.EqualFold(strings.TrimSpace(request.ProxyURL), "direct"):
			transport.Proxy = nil
		case strings.TrimSpace(request.ProxyURL) != "":
			parsed, err := url.Parse(strings.TrimSpace(request.ProxyURL))
			if err != nil {
				return nil, fmt.Errorf("parse WorkBuddy proxy URL: %w", err)
			}
			transport.Proxy = http.ProxyURL(parsed)
		default:
			transport.Proxy = http.ProxyFromEnvironment
		}
		return &http.Client{Transport: transport}, nil
	}
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{}
	}
	return client, nil
}

type conversationIDs struct {
	Conversation string
	Request      string
	Message      string
	Span         string
	Trace        string
}

func deriveConversationIDs(request ExecuteRequest, credential Credential) conversationIDs {
	digest := sha256Hex(string(request.Payload))
	requestID := hex32("workbuddy:request:" + strings.TrimSpace(request.Model) + ":" + digest)
	conversationID := requestID
	if key := strings.TrimSpace(request.ContinuityKey); key != "" {
		conversationID = hex32("workbuddy:conversation:" + key)
	}
	messageID := hex32("workbuddy:message:" + requestID + ":" + firstNonEmpty(request.AttemptID, "0"))
	return conversationIDs{
		Conversation: conversationID,
		Request:      requestID,
		Message:      messageID,
		Span:         hex16("workbuddy:span:" + messageID),
		Trace:        requestID,
	}
}

func chatHeaders(credential Credential, realm string, conversation conversationIDs) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("Accept", "application/json, text/event-stream")
	headers.Set("User-Agent", desktopUserAgent(realm))
	headers.Set("Accept-Language", languageFor(realm))
	headers.Set("X-Requested-With", "XMLHttpRequest")
	headers.Set("X-CodeBuddy-Request", "1")
	headers.Set("Origin", originFor(realm))
	headers.Set("Referer", originFor(realm)+"/")
	headers.Set("Authorization", "Bearer "+credential.AccessToken)
	headers.Set("X-User-Id", credential.UID)
	headers.Set("X-Machine-ID", stableID("machine", credential.UID, 36))
	headers.Set("X-Session-ID", stableID("session", credential.UID, 36))
	if strings.TrimSpace(credential.EnterpriseID) != "" {
		headers.Set("X-Enterprise-Id", credential.EnterpriseID)
	} else {
		headers.Set("X-No-Enterprise-Id", "1")
	}
	if domain := strings.TrimSpace(credential.Domain); domain != "" {
		headers.Set("X-Domain", strings.TrimPrefix(strings.TrimPrefix(domain, "https://"), "http://"))
	} else if realm == realmGlobal {
		headers.Set("X-Domain", globalDomain)
	} else {
		headers.Set("X-No-Department-Info", "1")
	}
	headers.Set("X-Agent-Purpose", "conversation")
	headers.Set("X-IDE-Name", "WorkBuddy")
	headers.Set("X-IDE-Type", "WorkBuddy")
	headers.Set("X-IDE-Version", clientVersion)
	headers.Set("X-Product", "WorkBuddy")
	if token := strings.TrimSpace(credential.DeviceToken); token != "" {
		headers.Set("X-Device-Token", token)
	}
	headers.Set("X-Conversation-ID", conversation.Conversation)
	headers.Set("X-Conversation-Request-ID", conversation.Request)
	headers.Set("X-Conversation-Message-ID", conversation.Message)
	headers.Set("X-Request-ID", conversation.Message)
	headers.Set("X-Root-Request-ID", conversation.Request)
	headers.Set("X-Trace-ID", conversation.Trace)
	headers.Set("X-B3-TraceId", conversation.Request)
	headers.Set("X-B3-SpanId", conversation.Span)
	headers.Set("X-B3-Sampled", "1")
	// 安全红线：chat 请求绝不携带 X-Refresh-Token。
	return headers
}

func stableID(purpose, uid string, length int) string {
	sum := sha256.Sum256([]byte("wb2a:" + purpose + ":" + strings.TrimSpace(uid)))
	encoded := hex.EncodeToString(sum[:])
	if length > 0 && length < len(encoded) {
		return encoded[:length]
	}
	return encoded
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func hex32(value string) string { return sha256Hex(value)[:32] }
func hex16(value string) string { return sha256Hex(value)[:16] }

// ---------------------------------------------------------------------------
// Request body normalization
// ---------------------------------------------------------------------------

func normalizeRequestBody(raw []byte, model string, credential Credential, conversation conversationIDs) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var input map[string]any
	if err := decoder.Decode(&input); err != nil {
		return nil, fmt.Errorf("WorkBuddy request body is not a JSON object: %w", err)
	}
	body := make(map[string]any)
	for _, key := range []string{
		"model", "messages", "stream", "stream_options", "max_tokens", "temperature", "top_p",
		"tools", "tool_choice", "reasoning_effort", "thinking", "prompt_cache_key", "conversation_id",
	} {
		if value, ok := input[key]; ok {
			body[key] = value
		}
	}
	if trimmed := firstNonEmpty(model, stringValue(body["model"])); trimmed != "" {
		body["model"] = trimmed
	}
	if _, ok := body["max_tokens"]; !ok {
		if value, ok := input["max_completion_tokens"]; ok {
			body["max_tokens"] = value
		}
	}
	normalizeMessages(body)
	sanitizeMessagesForUpstream(body)
	normalizeTools(body)
	normalizeToolChoice(body)
	normalizeMaxTokens(body)
	normalizeReasoning(body, stringValue(body["model"]))
	body["stream"] = true
	normalizeStreamOptions(body)
	if _, ok := body["prompt_cache_key"]; !ok {
		body["prompt_cache_key"] = "wb2a-" + stableID("cache", credential.UID, 8) + "-" + conversation.Conversation
	}
	return json.Marshal(body)
}

func normalizeMessages(body map[string]any) {
	rawMessages, _ := body["messages"].([]any)
	result := make([]any, 0, len(rawMessages)+1)
	var activeToolIDs map[string]struct{}
	for _, item := range rawMessages {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		role := stringValue(message["role"])
		if role == "developer" {
			role = "system"
			message["role"] = role
		}
		if role == "assistant" {
			activeToolIDs = map[string]struct{}{}
			if calls, ok := message["tool_calls"].([]any); ok {
				for _, call := range calls {
					entry, ok := call.(map[string]any)
					if !ok {
						continue
					}
					if id := stringValue(entry["id"]); id != "" {
						activeToolIDs[id] = struct{}{}
					}
				}
			}
		}
		if role == "tool" {
			id := stringValue(message["tool_call_id"])
			if _, ok := activeToolIDs[id]; !ok {
				continue
			}
			delete(activeToolIDs, id)
		}
		normalizeMessageContent(message)
		result = append(result, message)
	}
	if len(result) == 0 || stringValue(result[0].(map[string]any)["role"]) != "system" {
		result = append([]any{map[string]any{
			"role": "system", "content": "You are a helpful assistant.",
		}}, result...)
	}
	body["messages"] = result
}

func normalizeMessageContent(message map[string]any) {
	parts, ok := message["content"].([]any)
	if !ok {
		return
	}
	result := make([]any, 0, len(parts))
	for _, item := range parts {
		part, ok := item.(map[string]any)
		if !ok {
			continue
		}
		switch stringValue(part["type"]) {
		case "reasoning_text", "reasoning", "thinking":
			continue
		case "image_url":
			if text, ok := part["image_url"].(string); ok {
				part["image_url"] = map[string]any{"url": text}
			}
		}
		result = append(result, part)
	}
	if len(result) == 0 {
		message["content"] = ""
		return
	}
	message["content"] = result
}

func normalizeTools(body map[string]any) {
	tools, ok := body["tools"].([]any)
	if !ok {
		return
	}
	for _, item := range tools {
		tool, ok := item.(map[string]any)
		if !ok {
			continue
		}
		function, ok := tool["function"].(map[string]any)
		if !ok {
			continue
		}
		if parameters, ok := function["parameters"]; ok {
			function["parameters"] = normalizeSchema(parameters)
		}
	}
}

func normalizeSchema(value any) any {
	switch typed := value.(type) {
	case string:
		return strings.ReplaceAll(typed, `\_`, `_`)
	case []any:
		for index := range typed {
			typed[index] = normalizeSchema(typed[index])
		}
		return typed
	case map[string]any:
		for key := range typed {
			typed[key] = normalizeSchema(typed[key])
		}
		return typed
	default:
		return value
	}
}

func normalizeToolChoice(body map[string]any) {
	choice, ok := body["tool_choice"]
	if !ok {
		return
	}
	if text, ok := choice.(string); ok {
		if strings.EqualFold(strings.TrimSpace(text), "none") {
			delete(body, "tool_choice")
			delete(body, "tools")
		}
		return
	}
	object, ok := choice.(map[string]any)
	if !ok {
		delete(body, "tool_choice")
		return
	}
	switch strings.ToLower(stringValue(object["type"])) {
	case "auto":
		body["tool_choice"] = "auto"
	case "required", "any":
		body["tool_choice"] = "required"
	case "none":
		delete(body, "tool_choice")
		delete(body, "tools")
	case "function":
		if function, ok := object["function"].(map[string]any); ok {
			if name := stringValue(function["name"]); name != "" {
				body["tool_choice"] = name
				return
			}
		}
		body["tool_choice"] = "required"
	default:
		if name := stringValue(object["name"]); name != "" {
			body["tool_choice"] = name
			return
		}
		delete(body, "tool_choice")
	}
}

func normalizeMaxTokens(body map[string]any) {
	model := strings.ToLower(stringValue(body["model"]))
	if !strings.HasPrefix(model, "gpt-") {
		return
	}
	value, ok := body["max_tokens"]
	if !ok {
		return
	}
	number, ok := value.(json.Number)
	if !ok {
		return
	}
	parsed, err := number.Int64()
	if err != nil {
		return
	}
	if parsed < 16 {
		body["max_tokens"] = json.Number("16")
	}
}

func normalizeReasoning(body map[string]any, model string) {
	thinking, _ := body["thinking"].(map[string]any)
	disabled := thinking != nil && strings.EqualFold(stringValue(thinking["type"]), "disabled")
	if disabled {
		delete(body, "reasoning_effort")
		return
	}
	if strings.Contains(strings.ToLower(model), "deepseek") && thinking == nil {
		body["thinking"] = map[string]any{"type": "enabled"}
	}
	if _, ok := body["reasoning_effort"]; !ok {
		body["reasoning_effort"] = "high"
	}
}

func normalizeStreamOptions(body map[string]any) {
	options, ok := body["stream_options"].(map[string]any)
	if !ok {
		body["stream_options"] = map[string]any{"include_usage": true}
		return
	}
	if _, ok := options["include_usage"]; !ok {
		options["include_usage"] = true
	}
}

// ---------------------------------------------------------------------------
// Response normalization
// ---------------------------------------------------------------------------

func isEventStream(header http.Header) bool {
	return strings.Contains(strings.ToLower(header.Get("Content-Type")), "text/event-stream")
}

func sanitizedHeaders(header http.Header) http.Header {
	result := make(http.Header)
	for _, key := range []string{"Content-Type"} {
		if value := header.Get(key); value != "" {
			result.Set(key, value)
		}
	}
	return result
}

func looksLikeCompletion(body []byte) bool {
	var payload struct {
		Choices json.RawMessage `json:"choices"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return len(payload.Choices) > 0 && string(payload.Choices) != "null"
}

func normalizeStreamChunk(payload []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var chunk map[string]any
	if err := decoder.Decode(&chunk); err != nil || chunk == nil {
		return nil, errors.New("WorkBuddy stream chunk is not a JSON object")
	}
	changed := normalizeUsageField(chunk)
	if choices, ok := chunk["choices"].([]any); ok {
		for _, item := range choices {
			choice, ok := item.(map[string]any)
			if !ok {
				continue
			}
			for _, key := range []string{"delta", "message"} {
				delta, ok := choice[key].(map[string]any)
				if !ok {
					continue
				}
				if reasoning := stringValue(delta["reasoning"]); reasoning != "" {
					if stringValue(delta["reasoning_content"]) == "" {
						delta["reasoning_content"] = reasoning
					}
					delete(delta, "reasoning")
					changed = true
				}
			}
		}
	}
	if !changed {
		return payload, nil
	}
	return json.Marshal(chunk)
}

func normalizeUsageField(chunk map[string]any) bool {
	usage, ok := chunk["usage"].(map[string]any)
	if !ok {
		return false
	}
	hit, ok := usage["prompt_cache_hit_tokens"]
	if !ok {
		return false
	}
	details, ok := usage["prompt_tokens_details"].(map[string]any)
	if !ok {
		details = map[string]any{}
		usage["prompt_tokens_details"] = details
	}
	if _, ok := details["cached_tokens"]; ok {
		return false
	}
	details["cached_tokens"] = hit
	return true
}

func businessError(payload []byte) error {
	var envelope struct {
		Code    json.RawMessage `json:"code"`
		Msg     string          `json:"msg"`
		Error   json.RawMessage `json:"error"`
		Choices json.RawMessage `json:"choices"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil
	}
	if len(envelope.Choices) > 0 && string(envelope.Choices) != "null" {
		return nil
	}
	code := codeText(envelope.Code)
	hasError := len(envelope.Error) > 0 && string(envelope.Error) != "null"
	if !hasError && (code == "" || code == "0" || code == "200") {
		return nil
	}
	err := classifyHTTPError(http.StatusOK, payload, nil)
	if err == nil {
		err = &UpstreamError{Status: http.StatusBadGateway, Code: code, Message: envelope.Msg}
	}
	return err
}

func emptyStreamError() *UpstreamError {
	return &UpstreamError{
		Status: http.StatusBadGateway, Code: "empty_stream",
		Message: "WorkBuddy returned an empty stream",
	}
}

var errStreamDone = errors.New("workbuddy stream done")

func consumeSSE(reader io.Reader, emit func([]byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var data []string
	flush := func() error {
		if len(data) == 0 {
			return nil
		}
		payload := strings.Join(data, "\n")
		data = data[:0]
		trimmed := strings.TrimSpace(payload)
		if trimmed == "" {
			return nil
		}
		if trimmed == "[DONE]" {
			return errStreamDone
		}
		return emit([]byte(payload))
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(line, "data:")
			value = strings.TrimPrefix(value, " ")
			data = append(data, value)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return scanner.Err()
}

// ---------------------------------------------------------------------------
// Non-streaming aggregation
// ---------------------------------------------------------------------------

type aggregatedToolCall struct {
	ID        string
	Type      string
	Name      string
	Arguments strings.Builder
}

type aggregator struct {
	id           string
	created      int64
	model        string
	content      strings.Builder
	reasoning    strings.Builder
	toolCalls    map[int]*aggregatedToolCall
	finishReason string
	usage        json.RawMessage
	saw          bool
}

func newAggregator(model string) *aggregator {
	return &aggregator{model: model, toolCalls: map[int]*aggregatedToolCall{}}
}

type streamChunk struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Content          json.RawMessage `json:"content"`
			ReasoningContent string          `json:"reasoning_content"`
			ToolCalls        []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		Message      json.RawMessage `json:"message"`
		FinishReason *string         `json:"finish_reason"`
	} `json:"choices"`
	Usage json.RawMessage `json:"usage"`
}

func (aggregate *aggregator) consume(payload []byte) error {
	var chunk streamChunk
	if err := json.Unmarshal(payload, &chunk); err != nil {
		return nil
	}
	aggregate.saw = true
	if aggregate.id == "" {
		aggregate.id = chunk.ID
	}
	if aggregate.created == 0 {
		aggregate.created = chunk.Created
	}
	if aggregate.model == "" {
		aggregate.model = chunk.Model
	}
	if len(chunk.Usage) > 0 && string(chunk.Usage) != "null" {
		aggregate.usage = append(aggregate.usage[:0], chunk.Usage...)
	}
	for _, choice := range chunk.Choices {
		if text := contentText(choice.Delta.Content); text != "" {
			aggregate.content.WriteString(text)
		}
		if choice.Delta.ReasoningContent != "" {
			aggregate.reasoning.WriteString(choice.Delta.ReasoningContent)
		}
		for _, call := range choice.Delta.ToolCalls {
			entry, ok := aggregate.toolCalls[call.Index]
			if !ok {
				entry = &aggregatedToolCall{}
				aggregate.toolCalls[call.Index] = entry
			}
			if call.ID != "" {
				entry.ID = call.ID
			}
			if call.Type != "" {
				entry.Type = call.Type
			}
			if call.Function.Name != "" {
				entry.Name = call.Function.Name
			}
			entry.Arguments.WriteString(call.Function.Arguments)
		}
		if choice.FinishReason != nil && *choice.FinishReason != "" {
			aggregate.finishReason = *choice.FinishReason
		}
	}
	return nil
}

func (aggregate *aggregator) finish() ([]byte, error) {
	if aggregate.id == "" {
		aggregate.id = "chatcmpl-workbuddy"
	}
	if aggregate.created == 0 {
		aggregate.created = time.Now().Unix()
	}
	toolCalls := make([]any, 0, len(aggregate.toolCalls))
	for index := 0; index < len(aggregate.toolCalls); index++ {
		entry, ok := aggregate.toolCalls[index]
		if !ok {
			continue
		}
		callType := entry.Type
		if callType == "" {
			callType = "function"
		}
		toolCalls = append(toolCalls, map[string]any{
			"id": entry.ID, "type": callType,
			"function": map[string]any{"name": entry.Name, "arguments": entry.Arguments.String()},
		})
	}
	var content any
	if aggregate.content.Len() > 0 || len(toolCalls) == 0 {
		content = aggregate.content.String()
	}
	message := map[string]any{"role": "assistant", "content": content}
	if aggregate.reasoning.Len() > 0 {
		message["reasoning_content"] = aggregate.reasoning.String()
	}
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
	}
	finishReason := aggregate.finishReason
	if finishReason == "" {
		if len(toolCalls) > 0 {
			finishReason = "tool_calls"
		} else {
			finishReason = "stop"
		}
	}
	result := map[string]any{
		"id": aggregate.id, "object": "chat.completion", "created": aggregate.created,
		"model": aggregate.model,
		"choices": []any{map[string]any{
			"index": 0, "message": message, "finish_reason": finishReason,
		}},
	}
	if len(aggregate.usage) > 0 {
		result["usage"] = json.RawMessage(aggregate.usage)
	}
	return json.Marshal(result)
}

func contentText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err == nil {
		var builder strings.Builder
		for _, part := range parts {
			if part.Type == "text" {
				builder.WriteString(part.Text)
			}
		}
		return builder.String()
	}
	return ""
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

func classifyHTTPError(status int, body []byte, header http.Header) *UpstreamError {
	code, message, structured := parseErrorEnvelope(body)
	if status == http.StatusOK {
		status = httpStatusForCode(code, status)
	}
	if isCreditsExhausted(status, message) {
		code = "14018"
	}
	result := &UpstreamError{Status: status, Code: code, Message: truncate(message, 512)}
	if header != nil {
		result.retry = retryAfterOf(&http.Response{Header: header})
		if result.retry == 0 && (code == "6004" || isModelRateLimit(code, message)) {
			result.retry = modelRateLimitReset(message)
		}
	}
	normalized := normalizeErrorCode(code)
	switch {
	case status == http.StatusUnauthorized || normalized == "12153":
		result.Credential = true
	case status == http.StatusForbidden && !structured:
		// WAF/HTML 拦截：不惩罚账号。
	case normalized == "11140":
		if isModelRateLimit(code, message) || strings.Contains(strings.ToLower(message), "rate-limit") {
			result.Model = true
		} else {
			result.Credential = true
		}
	case normalized == "11128":
		// 11128 是请求本身的形态/内容问题（首条消息不是 system 或命中安全策略），
		// 换账号重试不会成功，直接判定为请求错误。
		result.Request = true
	case normalized == "11101", normalized == "11115", normalized == "11133", normalized == "11135":
		result.Request = true
	case normalized == "11102", normalized == "11158":
		result.Model = true
	case normalized == "6004":
		result.Model = true
	case normalized == "14018":
		// Credits are metered per model upstream: burning one model's pool
		// must not take the whole account out of rotation.
		result.Model = true
		if result.retry <= 0 {
			result.retry = 10 * time.Minute
		}
	case normalized == "14017":
		result.Credential = true
	case status == http.StatusTooManyRequests:
		result.Credential = true
	}
	return result
}

func parseErrorEnvelope(body []byte) (string, string, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.HasPrefix(trimmed, []byte("<")) {
		return "", "", false
	}
	var envelope struct {
		Code    json.RawMessage `json:"code"`
		Msg     string          `json:"msg"`
		Message string          `json:"message"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return "", "", false
	}
	code := codeText(envelope.Code)
	message := firstNonEmpty(envelope.Msg, envelope.Message)
	if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
		var nested struct {
			Code    json.RawMessage `json:"code"`
			Message string          `json:"message"`
			Msg     string          `json:"msg"`
			Data    struct {
				Code    json.RawMessage `json:"code"`
				Msg     string          `json:"msg"`
				Message string          `json:"message"`
			} `json:"data"`
		}
		if err := json.Unmarshal(envelope.Error, &nested); err == nil {
			if value := codeText(nested.Data.Code); value != "" {
				code = value
			} else if value := codeText(nested.Code); value != "" {
				code = value
			}
			if value := firstNonEmpty(nested.Data.Message, nested.Data.Msg, nested.Message, nested.Msg); value != "" {
				message = value
			}
		}
	}
	return code, message, true
}

func normalizeErrorCode(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	code = strings.ReplaceAll(code, "-", "")
	return code
}

func isCreditsExhausted(status int, message string) bool {
	if status != http.StatusTooManyRequests {
		return false
	}
	lower := strings.ToLower(message)
	return strings.Contains(lower, "credits exhausted") || strings.Contains(lower, "积分")
}

func isModelRateLimit(code, message string) bool {
	if normalizeErrorCode(code) == "6004" {
		return true
	}
	lower := strings.ToLower(message)
	return strings.Contains(lower, "rate-limiting") || strings.Contains(lower, "rate limit")
}

func modelRateLimitReset(message string) time.Duration {
	// 上游文案使用 UTC+8；只取第一个可解析时间戳。
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if index := strings.Index(message, layout[:10]); index >= 0 {
			trimmed := strings.TrimSpace(message[index:])
			if len(trimmed) >= len(layout) {
				trimmed = trimmed[:len(layout)]
			}
			location := time.FixedZone("UTC+8", 8*60*60)
			if when, err := time.ParseInLocation(layout, trimmed, location); err == nil {
				if delay := time.Until(when); delay > 0 {
					return delay
				}
			}
		}
	}
	return 0
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
