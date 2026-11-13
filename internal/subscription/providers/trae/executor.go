package trae

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
	"unicode/utf8"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	maxChatResponseBytes = 1 << 20
	traceHeader          = "X-Request-Id"
)

// ExecuteRequest 与 provider 无关的执行请求契约（与 WorkBuddy 对齐）。
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

// ExecuteResponse 是聚合后的完整响应。
type ExecuteResponse struct {
	Payload []byte
	Headers http.Header
}

// StreamChunk 是一个已翻译的 OpenAI chunk。
type StreamChunk struct {
	Payload    []byte
	Incomplete bool
	Err        error
}

// StreamResponse 是打开的 SSE 流。
type StreamResponse struct {
	Headers http.Header
	Chunks  <-chan StreamChunk
}

// Executor 执行 Trae 聊天补全。
type Executor interface {
	Execute(context.Context, string, Credential, ExecuteRequest) (ExecuteResponse, error)
	ExecuteStream(context.Context, string, Credential, ExecuteRequest) (*StreamResponse, error)
}

type upstreamExecutor struct{ profile Profile }

// NewExecutor 按区域构造执行器。
func NewExecutor(profile Profile) Executor { return &upstreamExecutor{profile: profile} }

// UpstreamError 携带受限的上游失败分类。
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
		return fmt.Sprintf("Trae upstream rejected the request (status=%d code=%s)", err.Status, code)
	case err.Status != 0:
		return fmt.Sprintf("Trae upstream rejected the request (status=%d)", err.Status)
	case code != "":
		return fmt.Sprintf("Trae upstream rejected the request (code=%s)", code)
	default:
		return "Trae upstream rejected the request"
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
			return ExecuteResponse{}, &UpstreamError{
				Status: http.StatusBadGateway, Code: "unexpected_response",
				Message: "Trae returned a non-JSON response",
			}
		}
		return ExecuteResponse{Payload: body, Headers: sanitizedHeaders(response.Header)}, nil
	}
	state := newStreamState(credential, request)
	aggregate := newAggregator(request.Model)
	err = consumeSSE(response.Body, func(event string, payload []byte) error {
		switch event {
		case "metadata":
			return nil
		case "output":
			delta, deltaErr := parseOutput(payload)
			if deltaErr != nil {
				return nil
			}
			return aggregate.consume(delta)
		case "token_usage":
			aggregate.consumeUsage(payload)
			return nil
		case "done":
			aggregate.finishReason = finishReasonOf(payload)
			return errStreamDone
		case "error":
			return upstreamErrorFromEvent(payload)
		default:
			return nil
		}
	})
	if err != nil && !errors.Is(err, errStreamDone) {
		return ExecuteResponse{}, err
	}
	if !aggregate.saw {
		return ExecuteResponse{}, emptyStreamError()
	}
	payload, err := aggregate.finish(state)
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
	state := newStreamState(credential, request)
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
			send(StreamChunk{Payload: body})
			return
		}
		// 上游逐 token 下发 reasoning，客户端思考面板会一词一行；合并后再转发。
		reasoning := newReasoningBuffer(func(text string) bool {
			return send(StreamChunk{Payload: state.chunk(map[string]any{"reasoning_content": text})})
		})
		saw := false
		streamErr := consumeSSE(response.Body, func(event string, payload []byte) error {
			switch event {
			case "metadata":
				return nil
			case "output":
				delta, deltaErr := parseOutput(payload)
				if deltaErr != nil {
					return nil
				}
				saw = true
				if !reasoning.Push(delta.reasoning) {
					return context.Canceled
				}
				if delta.content == "" && len(delta.toolCalls) == 0 {
					return nil
				}
				if !reasoning.Flush() {
					return context.Canceled
				}
				return emitDelta(send, state, delta)
			case "token_usage":
				if !reasoning.Flush() {
					return context.Canceled
				}
				send(StreamChunk{Payload: state.usageChunk(payload)})
				return nil
			case "done":
				state.finishReason = finishReasonOf(payload)
				if !reasoning.Flush() {
					return context.Canceled
				}
				send(StreamChunk{Payload: state.finishChunk(state.finishReason)})
				return errStreamDone
			case "error":
				reasoning.Discard()
				return upstreamErrorFromEvent(payload)
			default:
				return nil
			}
		})
		reasoning.Flush()
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
		return nil, fmt.Errorf("Trae only supports OpenAI chat completions, got %q", format)
	}
	profile := executor.profile
	if strings.TrimSpace(credential.AccessToken) == "" {
		// 凭证缺失不是上游故障：直接按鉴权失败返回，让运行时走刷新流程。
		return nil, &UpstreamError{
			Status: http.StatusUnauthorized, Code: "missing_access_token",
			Credential: true, Message: ErrMissingAccessToken.Error(),
		}
	}
	base := trimBaseURL(firstNonEmpty(request.BaseURL, credential.BaseURL, profile.CoreURL))
	payload, err := normalizeRequestBody(request.Payload, request.Model, credential, profile)
	if err != nil {
		return nil, err
	}
	client, err := clientForRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	paths := []string{profile.ChatPath}
	if profile.ChatFallback != "" {
		paths = append(paths, profile.ChatFallback)
	}
	var lastErr error
	for _, path := range paths {
		httpRequest, buildErr := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(payload))
		if buildErr != nil {
			return nil, buildErr
		}
		applyHeaders(httpRequest.Header, chatHeaders(profile, credential, requestIdFor(request)))
		response, doErr := client.Do(httpRequest)
		if doErr != nil {
			lastErr = doErr
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, maxChatResponseBytes))
			_ = response.Body.Close()
			err := classifyHTTPError(response.StatusCode, body)
			// 版本/路径差异时换回退路径；业务错误直接返回。
			if response.StatusCode == http.StatusNotFound && path != paths[len(paths)-1] {
				lastErr = err
				continue
			}
			return nil, err
		}
		return response, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("Trae upstream did not return a response")
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
				return nil, err
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

// ---------------------------------------------------------------------------
// 请求体
// ---------------------------------------------------------------------------

func normalizeRequestBody(raw []byte, model string, credential Credential, profile Profile) ([]byte, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("Trae request body is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var input map[string]any
	if err := decoder.Decode(&input); err != nil {
		return nil, fmt.Errorf("Trae request body is not a JSON object: %w", err)
	}
	body := make(map[string]any)
	modelName := firstNonEmpty(model, stringValue(input["model"]))
	if modelName == "" {
		return nil, errors.New("Trae request is missing a model")
	}
	body["model_name"] = modelName
	messages, err := normalizeMessages(input["messages"])
	if err != nil {
		return nil, err
	}
	body["messages"] = messages
	// 上游恒为 SSE；开关只是透传。
	body["stream"] = true
	for _, key := range []string{"temperature", "top_p", "max_tokens", "stop", "presence_penalty", "frequency_penalty"} {
		if value, ok := input[key]; ok && value != nil {
			body[key] = value
		}
	}
	if _, ok := body["max_tokens"]; !ok {
		if value, ok := input["max_completion_tokens"]; ok && value != nil {
			body["max_tokens"] = value
		}
	}
	if tools, ok := input["tools"]; ok && tools != nil {
		body["tools"] = tools
		if choice, ok := input["tool_choice"]; ok && choice != nil {
			body["tool_choice"] = choice
		}
	}
	if session := strings.TrimSpace(credential.UID); session != "" {
		body["session_id"] = "trae2api-" + shortHash(session+"|"+modelName)
	}
	return json.Marshal(body)
}

type messagePart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// normalizeMessages 把 OpenAI messages 归一为 Trae 的 parts 数组形态。
func normalizeMessages(raw any) ([]any, error) {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		return nil, errors.New("Trae request messages must be a non-empty array")
	}
	result := make([]any, 0, len(items)+1)
	for _, item := range items {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(stringValue(message["role"])))
		switch role {
		case "developer":
			role = "system"
		case "system", "user", "assistant", "tool":
		default:
			role = "user"
		}
		parts := contentParts(message["content"])
		entry := map[string]any{"role": role, "content": parts}
		if role == "assistant" {
			if calls, ok := message["tool_calls"]; ok && calls != nil {
				entry["tool_calls"] = calls
			}
		}
		if role == "tool" {
			if id := stringValue(message["tool_call_id"]); id != "" {
				entry["tool_call_id"] = id
			}
		}
		result = append(result, entry)
	}
	if len(result) == 0 {
		return nil, errors.New("Trae request messages were empty after normalization")
	}
	return result, nil
}

func contentParts(raw any) []messagePart {
	switch typed := raw.(type) {
	case string:
		return []messagePart{{Type: "text", Text: typed}}
	case nil:
		return []messagePart{{Type: "text", Text: ""}}
	case []any:
		parts := make([]messagePart, 0, len(typed))
		for _, item := range typed {
			switch entry := item.(type) {
			case string:
				parts = append(parts, messagePart{Type: "text", Text: entry})
			case map[string]any:
				kind := stringValue(entry["type"])
				if kind == "" || kind == "text" || kind == "input_text" {
					parts = append(parts, messagePart{Type: "text", Text: stringValue(entry["text"])})
					continue
				}
				// 非文本 part（图片等）首版不发送，避免触发 4001。
			}
		}
		if len(parts) == 0 {
			parts = append(parts, messagePart{Type: "text", Text: ""})
		}
		return parts
	default:
		return []messagePart{{Type: "text", Text: stringValue(raw)}}
	}
}

// ---------------------------------------------------------------------------
// 请求头
// ---------------------------------------------------------------------------

func chatHeaders(profile Profile, value Credential, requestID string) http.Header {
	headers := accountHeaders(profile, value)
	headers.Set("Accept", "text/event-stream")
	headers.Set("x-plugin-channel", pluginChannel)
	headers.Set("X-App-Id", appID)
	if requestID != "" {
		headers.Set(traceHeader, requestID)
	}
	if profile.StoreCountry != "" {
		headers.Set("store-country-code", profile.StoreCountry)
	}
	return headers
}

func accountHeaders(profile Profile, value Credential) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("User-Agent", profile.UserAgent)
	headers.Set("X-App-Id", appID)
	headers.Set("x-uid", strings.TrimSpace(value.UID))
	headers.Set("X-Ide-Version", firstNonEmpty(value.AppVersion, profile.AppVersion))
	headers.Set("x-ide-version-code", firstNonEmpty(value.VersionCode, profile.VersionCode))
	headers.Set("X-Ide-Version-Type", "stable")
	headers.Set("Request-Traffic-Type", "prod")
	headers.Set("X-Device-Id", firstNonEmpty(value.DeviceID, deriveDeviceID(value.UID)))
	headers.Set("X-Machine-Id", firstNonEmpty(value.MachineID, deriveMachineID(value.UID)))
	headers.Set("X-Device-Type", firstNonEmpty(value.DeviceType, "windows"))
	headers.Set("X-Device-Brand", firstNonEmpty(value.DeviceBrand, "Generic"))
	headers.Set("X-Device-Cpu", firstNonEmpty(value.DeviceCPU, "x64"))
	headers.Set("X-Os-Version", firstNonEmpty(value.OSVersion, "10.0"))
	if token := strings.TrimSpace(value.AccessToken); token != "" {
		headers.Set("x-ide-token", token)
		headers.Set("X-Cloudide-Token", token)
		headers.Set("Authorization", profile.AuthPrefix+" "+token)
	}
	return headers
}

// deriveDeviceID 生成 19 位数字设备号（官方客户端复用 userId 的数字形态）。
func deriveDeviceID(uid string) string {
	sum := sha256.Sum256([]byte("trae:device:" + strings.TrimSpace(uid)))
	value := uint64(0)
	for index := 0; index < 8; index++ {
		value = value<<8 | uint64(sum[index])
	}
	digits := strconv.FormatUint(value%9_000_000_000_000_000_000, 10)
	for len(digits) < 19 {
		digits = "1" + digits
	}
	return digits
}

// deriveMachineID 生成 64 位 hex 机器码。
func deriveMachineID(uid string) string {
	sum := sha256.Sum256([]byte("trae:machine:" + strings.TrimSpace(uid)))
	return hex.EncodeToString(sum[:])
}

func requestIdFor(request ExecuteRequest) string {
	sum := sha256.Sum256([]byte(firstNonEmpty(request.AttemptID, "0") + "|" + strconv.FormatInt(time.Now().UnixNano(), 10)))
	return hex.EncodeToString(sum[:16])
}

func applyHeaders(target, source http.Header) {
	for key, values := range source {
		target[key] = append([]string(nil), values...)
	}
}

func newJSONRequest(ctx context.Context, endpoint string, body []byte) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return request, nil
}

// ---------------------------------------------------------------------------
// SSE 翻译
// ---------------------------------------------------------------------------

type outputDelta struct {
	content   string
	reasoning string
	toolCalls []any
}

func parseOutput(payload []byte) (outputDelta, error) {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return outputDelta{}, err
	}
	delta := outputDelta{
		content:   rawStringValue(event["response"]),
		reasoning: rawStringValue(event["reasoning_content"]),
	}
	if delta.reasoning == "" {
		delta.reasoning = rawStringValue(event["reasoningContent"])
	}
	if delta.content == "" {
		delta.content = rawStringValue(event["content"])
	}
	if calls, ok := event["tool_calls"].([]any); ok && len(calls) > 0 {
		delta.toolCalls = calls
	}
	return delta, nil
}

func emitDelta(send func(StreamChunk) bool, state *streamState, delta outputDelta) error {
	body := map[string]any{}
	if delta.content != "" {
		body["content"] = delta.content
	}
	if len(delta.toolCalls) > 0 {
		body["tool_calls"] = normalizeToolCalls(delta.toolCalls)
	}
	if len(body) == 0 {
		return nil
	}
	if !send(StreamChunk{Payload: state.chunk(body)}) {
		return context.Canceled
	}
	return nil
}

// normalizeToolCalls 保证每个 tool_call 都带 index，便于客户端拼接增量。
func normalizeToolCalls(calls []any) []any {
	result := make([]any, 0, len(calls))
	for index, item := range calls {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		normalized := make(map[string]any, len(entry)+1)
		for key, value := range entry {
			normalized[key] = value
		}
		if _, ok := normalized["index"]; !ok {
			normalized["index"] = index
		}
		if _, ok := normalized["type"]; !ok {
			normalized["type"] = "function"
		}
		result = append(result, normalized)
	}
	return result
}

type streamState struct {
	id           string
	created      int64
	model        string
	finishReason string
}

func newStreamState(credential Credential, request ExecuteRequest) *streamState {
	sum := sha256.Sum256([]byte(firstNonEmpty(request.AttemptID, "0") + "|" + credential.UID + "|" + request.Model))
	return &streamState{
		id:      "chatcmpl-" + hex.EncodeToString(sum[:8]),
		created: time.Now().Unix(),
		model:   firstNonEmpty(request.Model, "trae"),
	}
}

func (state *streamState) chunk(delta map[string]any) []byte {
	payload := map[string]any{
		"id": state.id, "object": "chat.completion.chunk", "created": state.created,
		"model":   state.model,
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": nil}},
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func (state *streamState) finishChunk(reason string) []byte {
	if reason == "" {
		reason = "stop"
	}
	payload := map[string]any{
		"id": state.id, "object": "chat.completion.chunk", "created": state.created, "model": state.model,
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": reason}},
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func (state *streamState) usageChunk(raw []byte) []byte {
	usage := normalizeUsage(raw)
	payload := map[string]any{
		"id": state.id, "object": "chat.completion.chunk", "created": state.created, "model": state.model,
		"choices": []any{}, "usage": usage,
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func normalizeUsage(raw []byte) map[string]any {
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return map[string]any{}
	}
	if nested, ok := event["usage"].(map[string]any); ok {
		return nested
	}
	usage := make(map[string]any, len(event))
	for key, value := range event {
		usage[key] = value
	}
	if cached, ok := usage["cache_read_tokens"]; ok {
		if _, exists := usage["cached_tokens"]; !exists {
			usage["cached_tokens"] = cached
		}
	}
	return usage
}

func finishReasonOf(payload []byte) string {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return "stop"
	}
	return firstNonEmpty(stringValue(event["finish_reason"]), stringValue(event["finishReason"]), "stop")
}

// ---------------------------------------------------------------------------
// reasoning 合并
// ---------------------------------------------------------------------------

type reasoningBuffer struct {
	builder strings.Builder
	emit    func(string) bool
}

func newReasoningBuffer(emit func(string) bool) *reasoningBuffer {
	return &reasoningBuffer{emit: emit}
}

// Push 累积 reasoning，达到阈值后一次性下发。
func (buffer *reasoningBuffer) Push(text string) bool {
	if text == "" {
		return true
	}
	buffer.builder.WriteString(text)
	if utf8.RuneCountInString(buffer.builder.String()) >= 24 {
		return buffer.Flush()
	}
	return true
}

func (buffer *reasoningBuffer) Flush() bool {
	if buffer.builder.Len() == 0 {
		return true
	}
	text := buffer.builder.String()
	buffer.builder.Reset()
	return buffer.emit(text)
}

func (buffer *reasoningBuffer) Discard() { buffer.builder.Reset() }

// ---------------------------------------------------------------------------
// 非流式聚合
// ---------------------------------------------------------------------------

type aggregatedToolCall struct {
	ID        string
	Type      string
	Name      string
	Arguments strings.Builder
}

type aggregator struct {
	content      strings.Builder
	reasoning    strings.Builder
	toolCalls    map[int]*aggregatedToolCall
	usage        map[string]any
	finishReason string
	saw          bool
}

func newAggregator(string) *aggregator {
	return &aggregator{toolCalls: map[int]*aggregatedToolCall{}}
}

func (aggregate *aggregator) consume(delta outputDelta) error {
	aggregate.saw = true
	aggregate.content.WriteString(delta.content)
	aggregate.reasoning.WriteString(delta.reasoning)
	for index, item := range delta.toolCalls {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		position := index
		if value := numeric(entry["index"]); value != 0 {
			position = int(value)
		}
		call, ok := aggregate.toolCalls[position]
		if !ok {
			call = &aggregatedToolCall{}
			aggregate.toolCalls[position] = call
		}
		if id := stringValue(entry["id"]); id != "" {
			call.ID = id
		}
		if kind := stringValue(entry["type"]); kind != "" {
			call.Type = kind
		}
		if function, ok := entry["function"].(map[string]any); ok {
			if name := stringValue(function["name"]); name != "" {
				call.Name = name
			}
			call.Arguments.WriteString(stringValue(function["arguments"]))
		}
	}
	return nil
}

func (aggregate *aggregator) consumeUsage(payload []byte) {
	aggregate.saw = true
	aggregate.usage = normalizeUsage(payload)
}

func (aggregate *aggregator) finish(state *streamState) ([]byte, error) {
	toolCalls := make([]any, 0, len(aggregate.toolCalls))
	for index := 0; index < len(aggregate.toolCalls); index++ {
		call, ok := aggregate.toolCalls[index]
		if !ok {
			continue
		}
		kind := call.Type
		if kind == "" {
			kind = "function"
		}
		toolCalls = append(toolCalls, map[string]any{
			"id": call.ID, "type": kind,
			"function": map[string]any{"name": call.Name, "arguments": call.Arguments.String()},
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
		"id": state.id, "object": "chat.completion", "created": state.created, "model": state.model,
		"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finishReason}},
	}
	if len(aggregate.usage) > 0 {
		result["usage"] = aggregate.usage
	}
	return json.Marshal(result)
}

// ---------------------------------------------------------------------------
// 流解析与错误
// ---------------------------------------------------------------------------

var errStreamDone = errors.New("trae stream done")

func consumeSSE(reader io.Reader, emit func(event string, payload []byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	event := ""
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
		return emit(event, []byte(payload))
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			event = ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch strings.TrimSpace(field) {
		case "event":
			event = strings.TrimSpace(value)
		case "data":
			data = append(data, value)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return scanner.Err()
}

func isEventStream(header http.Header) bool {
	return strings.Contains(strings.ToLower(header.Get("Content-Type")), "text/event-stream")
}

func sanitizedHeaders(header http.Header) http.Header {
	result := make(http.Header)
	if value := header.Get("Content-Type"); value != "" {
		result.Set("Content-Type", value)
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

func businessError(payload []byte) error {
	var envelope struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
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
	return classifyHTTPError(http.StatusOK, payload)
}

func upstreamErrorFromEvent(payload []byte) *UpstreamError {
	code := ""
	message := ""
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err == nil {
		code = firstNonEmpty(stringValue(event["code"]), stringValue(event["error_code"]))
		message = firstNonEmpty(stringValue(event["message"]), stringValue(event["error"]), stringValue(event["msg"]))
	}
	err := classifyHTTPError(http.StatusOK, payload)
	if err == nil {
		err = &UpstreamError{Status: http.StatusBadGateway, Code: code, Message: truncate(message, 512)}
	}
	if err.Code == "" {
		err.Code = code
	}
	return err
}

func emptyStreamError() *UpstreamError {
	return &UpstreamError{
		Status: http.StatusBadGateway, Code: "empty_stream",
		Message: "Trae returned an empty stream",
	}
}

func classifyHTTPError(status int, body []byte) *UpstreamError {
	code := normalizeErrorCode(codeOf(body))
	if status == http.StatusOK {
		status = httpStatusForCode(code, status)
	}
	result := &UpstreamError{Status: status, Code: code, Message: truncate(messageOf(body), 512)}
	switch code {
	case "1001", "20101":
		result.Credential = true
	case "4001", "3003":
		result.Request = true
	case "4015":
		// 模型权限/上游拒绝：换号不会成功，按主机错误返回，不惩罚账号。
		result.Request = true
	case "4019":
		// 风控命中：账号级冷却，直接返回，禁止换号重试。
		result.Credential = true
		result.retry = 10 * time.Minute
	case "4011":
		result.Credential = false
	case "4023":
		result.Model = true
	case "20403", "20405":
		result.Credential = true
	}
	if result.Status == 0 {
		result.Status = httpStatusForCode(code, http.StatusBadRequest)
	}
	return result
}

func httpStatusForCode(code string, fallback int) int {
	switch normalizeErrorCode(code) {
	case "1001", "20101", "20403", "20405":
		return http.StatusUnauthorized
	case "4015", "4019":
		return http.StatusForbidden
	case "4001", "3003":
		return http.StatusBadRequest
	case "4023":
		return http.StatusBadRequest
	case "4011":
		return http.StatusTooManyRequests
	case "", "0":
		return fallback
	default:
		return fallback
	}
}

func normalizeErrorCode(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	return strings.ReplaceAll(code, "-", "")
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

// rawStringValue 与 stringValue 的区别是不做裁剪：SSE 的正文增量可能是
// 纯空格，裁剪会破坏词间空格。
func rawStringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return stringValue(value)
}
