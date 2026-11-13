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
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	maxChatResponseBytes = 1 << 20
	traceHeader          = "X-Request-Id"
	// rawChatFunction 是 IDE 通道默认协议函数，必须同时出现在请求体
	// 与 X-App-Function / X-Ide-Function 头里。
	rawChatFunction = "chat_v3"

	// CN llm_raw_chat 的准入头组（与 TraeCode 参考实现一致）。
	chatAppIDCN         = appID
	chatAppVersionCN    = "default"
	chatIdeVersionCN    = "3.4.4"
	chatVersionCodeCN   = "20260212"
	chatPluginChannelCN = "desktop"
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
	payload, err := normalizeRequestBody(ctx, request.Payload, request.Model, credential, profile)
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

func normalizeRequestBody(ctx context.Context, raw []byte, model string, credential Credential, profile Profile) ([]byte, error) {
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
	// llm_raw_chat 要求 config_name 来自 batch_get_detail_param，model_name 是
	// 该配置下的真实模型 id（如 DeepSeek-V4-Flash__dev）；直接用展示名会被
	// 上游以 4001（param invalid）拒绝。
	resolved, found := resolveChatModel(ctx, profile, credential, modelName)
	if !found {
		// 目录里没有这个模型：可能是仅支持 Trae 客户端远程服务的新模型，
		// 也可能是已停用配置。不能再退化成裸参数去试探上游，否则用户只会
		// 看到 4001/4028 这类无法定位业务含义的错误。
		return nil, &UpstreamError{
			Status: http.StatusBadRequest,
			Code:   "model_not_in_catalog",
			Message: fmt.Sprintf(
				"模型 %q 不在当前 Trae 账号的 llm_raw_chat 目录中：它可能只在 Trae 客户端的远程服务里可用，网关暂不支持。",
				modelName,
			),
			Model: true,
		}
	}
	slog.Info("trae model resolved", "v", resolved.ConfigName+"|"+resolved.ModelName)
	modelName = resolved.ModelName
	body["config_name"] = resolved.ConfigName
	body["model_name"] = resolved.ModelName
	body["config_source"] = resolved.ConfigSource
	// 自建模型必须带 provider + custom_model_id + is_custom_model，否则上游
	// 只回 4001（param invalid）；带齐后由上游代理到对应模型服务商。
	if resolved.IsCustomModel {
		body["is_custom_model"] = true
		body["provider"] = resolved.Provider
		body["custom_model_id"] = resolved.CustomModelID
	}
	body["raw_chat_function"] = rawChatFunction
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
		if normalized := normalizeTools(tools); len(normalized) > 0 {
			body["tools"] = normalized
			if choice := normalizeToolChoice(input["tool_choice"]); choice != nil {
				body["tool_choice"] = choice
			}
			if parallel, ok := input["parallel_tool_calls"].(bool); ok {
				body["parallel_tool_calls"] = parallel
			}
		}
	}
	if session := credential.EffectiveUID(); session != "" {
		// conversation_id/session_id 是 UUID 类型参数，非 UUID 串会被上游
		// 判为 4001 param invalid；按账号+模型+函数做稳定映射。
		sessionID := stableSessionUUID(session + "\x1f" + modelName + "\x1f" + rawChatFunction)
		body["session_id"] = sessionID
		body["conversation_id"] = sessionID
	}
	return json.Marshal(body)
}

type messagePart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// normalizeTools 把 OpenAI tools 归一成 llm_raw_chat 接受的形态：function
// 的 parameters 必须是 JSON 字符串，传对象会被上游以 400 空响应拒绝
// （参考 Magpie 的 nativeTools 实现）。
func normalizeTools(raw any) []any {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	result := make([]any, 0, len(items))
	for _, item := range items {
		tool, ok := item.(map[string]any)
		if !ok {
			continue
		}
		function, ok := tool["function"].(map[string]any)
		if !ok {
			// 兼容 {name, description, parameters} 的扁平写法。
			function = tool
		}
		name := strings.TrimSpace(stringValue(function["name"]))
		if name == "" {
			continue
		}
		entry := map[string]any{"type": "function"}
		normalized := map[string]any{"name": name, "description": stringValue(function["description"])}
		parameters := function["parameters"]
		if parameters == nil {
			parameters = function["input_schema"]
		}
		if _, isString := parameters.(string); !isString {
			if parameters == nil {
				parameters = map[string]any{}
			}
			encoded, err := json.Marshal(parameters)
			if err != nil {
				continue
			}
			parameters = string(encoded)
		}
		normalized["parameters"] = parameters
		if strict, ok := function["strict"].(bool); ok {
			normalized["strict"] = strict
		}
		entry["function"] = normalized
		result = append(result, entry)
	}
	return result
}

// normalizeToolChoice 把对象形态的 tool_choice 降级成上游接受的字符串。
func normalizeToolChoice(raw any) any {
	switch value := raw.(type) {
	case string:
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed == "" {
			return nil
		}
		return trimmed
	case map[string]any:
		kind := strings.ToLower(strings.TrimSpace(stringValue(value["type"])))
		if kind == "function" {
			if function, ok := value["function"].(map[string]any); ok {
				if name := strings.TrimSpace(stringValue(function["name"])); name != "" {
					return name
				}
			}
			return "auto"
		}
		if kind != "" {
			return kind
		}
	}
	return nil
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
			// 上游历史里的调用是 function_call 形态，OpenAI 客户端回放的是
			// function 形态，不转换会让模型看不到自己之前的调用。
			if calls, ok := message["tool_calls"].([]any); ok && len(calls) > 0 {
				converted := make([]any, 0, len(calls))
				for _, call := range calls {
					item, ok := call.(map[string]any)
					if !ok {
						continue
					}
					function, _ := item["function"].(map[string]any)
					converted = append(converted, map[string]any{
						"id":   stringValue(item["id"]),
						"type": firstNonEmpty(stringValue(item["type"]), "function"),
						"function_call": map[string]any{
							"name":      stringValue(function["name"]),
							"arguments": firstNonEmpty(rawStringValue(function["arguments"]), "{}"),
						},
					})
				}
				if len(converted) > 0 {
					entry["tool_calls"] = converted
				}
			}
		}
		if role == "tool" {
			if id := stringValue(message["tool_call_id"]); id != "" {
				entry["tool_call_id"] = id
			}
			if name := stringValue(message["name"]); name != "" {
				entry["name"] = name
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

// stableSessionUUID 把任意会话材料映射成稳定的 UUID 字符串。
func stableSessionUUID(material string) string {
	sum := sha256.Sum256([]byte(material))
	hexValue := fmt.Sprintf("%x", sum[:16])
	return hexValue[0:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" +
		hexValue[16:20] + "-" + hexValue[20:32]
}

// chatModelConfig 是一条可用的模型配置：config_name + 真实 model_name + 来源。
// chatModelDetail 是目录里单个模型变体的原始描述。
type chatModelDetail struct {
	ModelName    string `json:"model_name"`
	Provider     string `json:"provider"`
	CustomConfig struct {
		CustomModelID json.Number `json:"custom_model_id"`
		Status        bool        `json:"status"`
	} `json:"custom_config"`
}

// hasProviderBinding 判断自建条目是否带齐调用所需的 provider 与 custom_model_id。
// 已停用的模型（客户端关 key 后 custom_config.status=false）不算可用绑定。
func hasProviderBinding(details []chatModelDetail) bool {
	for _, detail := range details {
		if detail.CustomConfig.Status &&
			strings.TrimSpace(detail.Provider) != "" && numeric(detail.CustomConfig.CustomModelID) > 0 {
			return true
		}
	}
	return false
}

type chatModelConfig struct {
	ConfigName   string
	ModelName    string
	ConfigSource int
	// 自建模型（config_source=3）必须额外带 provider 与 custom_model_id，
	// 否则 llm_raw_chat 会返回 4001；带齐后由上游代理到模型服务商。
	Provider      string
	CustomModelID int64
	IsCustomModel bool
}

type cachedChatCatalog struct {
	fetchedAt time.Time
	models    map[string]chatModelConfig
}

const (
	chatCatalogPath = "/api/ide/v1/batch_get_detail_param"
	chatCatalogTTL  = 10 * time.Minute
)

var (
	chatCatalogCache sync.Map // 账号 -> cachedChatCatalog
	chatCatalogFuncs = []string{
		"ui_builder_v2", "solo_coder", "chat_v3", "solo_builder", "builder_v3", "builder",
		"chat", "inline_chat", "git_ai", "custom_agent_generation", "utils", "code_reviewer",
		"code_review_summary", "solo_agent", "solo_agent_remote", "solo_work_remote",
		"solo_agent_lite", "solo_work_lite", "solo_design_lite", "solo_design_remote",
		"multimodal", "system_diagnosis",
	}
)

// chatCatalogRequest 是 batch_get_detail_param 的入参（客户端同款）。
func chatCatalogRequest() map[string]any {
	return map[string]any{
		"functions": chatCatalogFuncs, "agent_type": "",
		"current_config_info": map[string]any{"config_name": "", "is_custom_model": false},
		"mode_type":           0, "access_type": 0, "ab_force_vids": "",
		"ab_autotest_advanced_mode": 0, "show_custom_model": true,
	}
}

// resolveChatModel 把客户端传入的模型名解析成上游要求的配置三元组。
// 目录可用但模型不在其中时返回 ok=false：此时不应再用原值去试探上游，
// 否则已停用的自建配置会以 4028、远程服务专属模型会以 4001 结束。
// 只有目录本身拉取失败时才退回原值，保持旧行为。
func resolveChatModel(ctx context.Context, profile Profile, value Credential, model string) (chatModelConfig, bool) {
	trimmed := strings.TrimSpace(model)
	fallback := chatModelConfig{ConfigName: trimmed, ModelName: trimmed, ConfigSource: 1}
	key := strings.ToLower(trimmed)
	if key == "" {
		return fallback, true
	}
	if strings.TrimSpace(value.AccessToken) == "" {
		return fallback, true
	}
	cacheKey := firstNonEmpty(strings.TrimSpace(value.UID), strings.TrimSpace(value.DeviceID))
	if entry, ok := chatCatalogCache.Load(cacheKey); ok {
		cached := entry.(cachedChatCatalog)
		if time.Since(cached.fetchedAt) < chatCatalogTTL {
			if resolved, hit := cached.models[key]; hit {
				return resolved, true
			}
			slog.Warn("trae model missing from catalog", "model", key, "size", len(cached.models))
			return chatModelConfig{}, false
		}
	}
	models, err := fetchChatCatalog(ctx, profile, value)
	if err != nil {
		slog.Warn("trae model catalog unavailable", "realm", profile.Key, "error", err)
		return fallback, true
	}
	chatCatalogCache.Store(cacheKey, cachedChatCatalog{fetchedAt: time.Now(), models: models})
	if resolved, hit := models[key]; hit {
		return resolved, true
	}
	slog.Warn("trae model missing from catalog", "model", key, "size", len(models))
	return chatModelConfig{}, false
}

// fetchChatCatalog 拉取账号可用的模型目录，建立 展示名/配置名/真实模型名 的索引。
func fetchChatCatalog(ctx context.Context, profile Profile, value Credential) (map[string]chatModelConfig, error) {
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil || client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	base := strings.TrimRight(strings.TrimSpace(profile.CoreURL), "/")
	if base == "" {
		return nil, errors.New("Trae 渠道缺少核心服务地址")
	}
	requestBody, err := json.Marshal(chatCatalogRequest())
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+chatCatalogPath, bytes.NewReader(requestBody))
	if err != nil {
		return nil, err
	}
	for name, values := range chatHeaders(profile, value, "") {
		request.Header[name] = values
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("model catalog rejected (status=%d code=%s)", response.StatusCode, codeOf(payload))
	}
	var parsed struct {
		FunctionConfigs []struct {
			Function       string `json:"function"`
			ConfigInfoList []struct {
				ConfigName    string `json:"config_name"`
				ConfigSource  int    `json:"config_source"`
				ConfigSwitch  bool   `json:"config_switch"`
				DisplayConfig struct {
					DisplayName     string `json:"display_name"`
					IsCustomModel   bool   `json:"is_custom_model"`
					ModelCapability string `json:"model_capability"`
				} `json:"display_config"`
				ModelDetailList []chatModelDetail `json:"model_detail_list"`
			} `json:"config_info_list"`
		} `json:"function_configs"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, err
	}
	models := make(map[string]chatModelConfig)
	aliases := make(map[string]chatModelConfig)
	// 精确键（config_name / model_name）与展示名分表存放：官方目录里存在
	// DeepSeek-V4-Flash 与 DeepSeek-V4-Flash-Official 共用同一个展示名，
	// 若混在一张表里，后写入的展示名别名会把精确键覆盖成另一个配置，
	// 请求就会打到当前套餐不可用的配置上（上游返回 code=1005）。
	put := func(target map[string]chatModelConfig, key string, candidate chatModelConfig) {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			return
		}
		// 官方配置优先：同名时账号自建模型（source=3）不能覆盖 source=1。
		if existing, ok := target[key]; ok &&
			existing.ConfigSource == 1 && candidate.ConfigSource != 1 {
			return
		}
		target[key] = candidate
	}
	for _, function := range parsed.FunctionConfigs {
		// 只取 chat 通道的配置：其它 function 会有同名但不同 model_name 的条目，
		// 混进来会把 llm_raw_chat 需要的配置覆盖掉。
		if function.Function != rawChatFunction {
			continue
		}
		for _, info := range function.ConfigInfoList {
			if info.ConfigName == "" {
				continue
			}
			// 官方条目要求有 model_capability；没有的是 summary/title_generation/
			// fast_apply 等内部工具，不是对话模型。自建条目（source=3）要求
			// 模型详情里带 provider 与 custom_model_id，缺一不可——带齐后
			// llm_raw_chat 会代理到模型服务商，缺了只会返回 4001。
			custom := info.ConfigSource != 1
			if custom {
				// 客户端关闭自建模型的 key 后，条目不会从目录消失，只会把
				// config_switch 与 custom_config.status 置为 false。这类配置
				// 仍能通过参数校验，但上游必然以 4028 拒绝，因此必须在目录
				// 阶段排除，不能让它们参与任何模型名解析。
				if !info.ConfigSwitch {
					continue
				}
				if !info.DisplayConfig.IsCustomModel || !hasProviderBinding(info.ModelDetailList) {
					continue
				}
			} else if info.DisplayConfig.IsCustomModel ||
				strings.TrimSpace(info.DisplayConfig.ModelCapability) == "" {
				continue
			}
			resolved := chatModelConfig{
				ConfigName:    info.ConfigName,
				ModelName:     info.ConfigName,
				ConfigSource:  info.ConfigSource,
				IsCustomModel: custom,
			}
			if custom {
				for _, detail := range info.ModelDetailList {
					if !detail.CustomConfig.Status ||
						strings.TrimSpace(detail.Provider) == "" ||
						numeric(detail.CustomConfig.CustomModelID) == 0 {
						continue
					}
					resolved.Provider = strings.TrimSpace(detail.Provider)
					resolved.CustomModelID = numeric(detail.CustomConfig.CustomModelID)
					break
				}
			}
			for _, detail := range info.ModelDetailList {
				if custom && !detail.CustomConfig.Status {
					continue
				}
				name := strings.TrimSpace(detail.ModelName)
				if name == "" {
					continue
				}
				// 配置名必须映射到真实 model_name（如 xxx__dev），
				// 直接发配置名会被上游判为 4023（model unknown）。
				if resolved.ModelName == info.ConfigName {
					resolved.ModelName = name
				}
				candidate := resolved
				candidate.ModelName = name
				if custom {
					candidate.Provider = strings.TrimSpace(detail.Provider)
					candidate.CustomModelID = numeric(detail.CustomConfig.CustomModelID)
				}
				put(models, name, candidate)
			}
			put(models, info.ConfigName, resolved)
			if info.DisplayConfig.DisplayName != "" {
				put(aliases, info.DisplayConfig.DisplayName, resolved)
			}
		}
	}
	for key, value := range aliases {
		if _, exists := models[key]; !exists {
			models[key] = value
		}
	}
	slog.Info("trae chat catalog loaded", "realm", profile.Key, "models", len(models))
	return models, nil
}

// detailParamModels 把目录去重成对外模型列表（以 config_name 作为 id）。
func detailParamModels(ctx context.Context, profile Profile, value Credential) ([]modelEntry, error) {
	models, err := fetchChatCatalog(ctx, profile, value)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(models))
	result := make([]modelEntry, 0, len(models))
	for _, config := range models {
		if config.ConfigName == "" {
			continue
		}
		// 自建模型现在可以被解析和调用（provider + custom_model_id），但账号
		// 侧的服务商密钥是否有效只有在真实请求时才知道；默认不放进自动
		// 目录，避免列表里出现必然 4028 的条目。需要时按 config_name 手工
		// 加入分组即可，网关会带上自建参数。
		if config.IsCustomModel {
			continue
		}
		if _, exists := seen[config.ConfigName]; exists {
			continue
		}
		seen[config.ConfigName] = struct{}{}
		result = append(result, modelEntry{ID: config.ConfigName, Name: config.ConfigName})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ID < result[right].ID })
	return result, nil
}

func chatHeaders(profile Profile, value Credential, requestID string) http.Header {
	headers := accountHeaders(profile, value)
	headers.Set("Accept", "text/event-stream")
	headers.Set("x-plugin-channel", pluginChannel)
	headers.Set("X-App-Id", appID)
	headers.Set("X-App-Function", rawChatFunction)
	headers.Set("X-Ide-Function", rawChatFunction)
	headers.Set("X-App-Version", firstNonEmpty(value.AppVersion, profile.AppVersion))
	headers.Set("X-App-Version-Code", firstNonEmpty(value.VersionCode, profile.VersionCode))
	if profile.Key == realmCN {
		// CN 的 llm_raw_chat 把版本头当作模型准入条件，版本过低会直接 4001；
		// 这里对齐 TraeCode 通道的固定组合（X-App-Version 必须是 default）。
		headers.Set("X-App-Id", chatAppIDCN)
		headers.Set("X-App-Version", chatAppVersionCN)
		headers.Set("X-App-Version-Code", chatVersionCodeCN)
		headers.Set("X-Ide-Version", chatIdeVersionCN)
		headers.Set("X-Ide-Version-Code", chatVersionCodeCN)
		headers.Set("X-Ide-Version-Type", "stable")
		headers.Set("X-Plugin-Channel", chatPluginChannelCN)
	}
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
	if uid := value.EffectiveUID(); uid != "" && !strings.HasPrefix(uid, "rt-") {
		// rt- 开头是身份补全失败时的占位值，上游会把它当非法参数。
		// 上游按原始大小写读取该头；Header.Set 会规范成 X-Uid，
		// 因此直接写 map 保持小写。
		headers["x-uid"] = []string{uid}
	}
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
		delta.toolCalls = normalizeUpstreamToolCalls(calls)
	}
	return delta, nil
}

// normalizeUpstreamToolCalls 把上游的 function_call 形态翻译成 OpenAI 的
// function 形态。上游下发的是：
// {"index":0,"id":"call_x","type":"function",
//
//	"function_call":{"name":"get_time","arguments":"{\"city\":\"Tokyo\"}"}}
func normalizeUpstreamToolCalls(calls []any) []any {
	result := make([]any, 0, len(calls))
	for index, item := range calls {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		position := index
		if value := numeric(entry["index"]); value != 0 {
			position = int(value)
		}
		normalized := map[string]any{
			"index": position,
			"type":  firstNonEmpty(stringValue(entry["type"]), "function"),
		}
		if id := stringValue(entry["id"]); id != "" {
			normalized["id"] = id
		}
		function := map[string]any{"name": "", "arguments": ""}
		if call, ok := entry["function_call"].(map[string]any); ok {
			function["name"] = stringValue(call["name"])
			function["arguments"] = firstNonEmpty(
				rawStringValue(call["arguments"]),
				rawStringValue(call["partial_arguments"]),
			)
		} else if call, ok := entry["function"].(map[string]any); ok {
			function["name"] = stringValue(call["name"])
			function["arguments"] = rawStringValue(call["arguments"])
		}
		normalized["function"] = function
		result = append(result, normalized)
	}
	return result
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
	// 上游 4xx 的原始 message 是唯一能区分“消息格式 / 模型名”的证据来源。
	slog.Warn("trae upstream rejected chat request",
		"status", status, "code", code, "message", result.Message,
		"body", truncate(string(body), 512))
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
	case "4028":
		// 自建模型已被上游路由到服务商，但服务商密钥无效/过期；
		// 属于账号配置问题，重试与换号都无效。
		result.Request = true
	case "1005":
		// 账号套餐不允许该模型配置（extra 里带 plan）；换号通常无效，
		// 按模型级错误返回，不惩罚账号。
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
	case "4015", "4019", "1005":
		return http.StatusForbidden
	case "4028":
		return http.StatusBadGateway
	case "4001", "3003", "2001":
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
