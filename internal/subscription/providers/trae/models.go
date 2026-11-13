package trae

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"gpt-load/internal/channel/spec"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

// modelEntry 把可调用的上游模型名与展示名配对。
type modelEntry struct {
	ID   string
	Name string
}

// fallbackCatalog 是动态目录不可用时的内置快照。
// 国际版 model_list 会返回 403 no schema found，必须靠快照兜底；
// 国内版仅在动态拉取失败时使用，避免把网络抖动当成账号异常。
var fallbackCatalog = map[string][]modelEntry{
	realmCN: {
		{ID: "glm-5.2", Name: "GLM-5.2"},
		{ID: "kimi-k2", Name: "Kimi K2"},
		{ID: "deepseek-v3.2", Name: "DeepSeek V3.2"},
		{ID: "Doubao-Seed-1.6", Name: "Doubao Seed 1.6"},
	},
	realmIntl: {
		{ID: "gemini-2.5-pro-latest", Name: "Gemini 2.5 Pro"},
		{ID: "gemini_3.1_pro", Name: "Gemini 3.1 Pro"},
		{ID: "aws_claude_4.7_opus", Name: "Claude 4.7 Opus"},
		{ID: "aws_claude_4.6_sonnet", Name: "Claude 4.6 Sonnet"},
		{ID: "gpt-5.5", Name: "GPT-5.5"},
		{ID: "gpt-6-astra", Name: "GPT-6 Astra"},
		{ID: "groK-4", Name: "Grok 4"},
		{ID: "kimi-k2", Name: "Kimi K2"},
	},
}

// UpstreamHTTPError 描述模型目录请求的失败，不携带响应体。
type UpstreamHTTPError struct {
	Operation  string
	StatusCode int
}

func (err *UpstreamHTTPError) Error() string {
	return fmt.Sprintf("Trae %s returned status %d", err.Operation, err.StatusCode)
}

// ListModels 返回账号可用的模型 id 列表。
func ListModels(ctx context.Context, profile Profile, value Credential, baseURL string) ([]string, error) {
	entries, err := ListNamedModels(ctx, profile, value, baseURL)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.ID)
	}
	return result, nil
}

// ListNamedModels 返回带展示名的模型目录。
func ListNamedModels(ctx context.Context, profile Profile, value Credential, baseURL string) ([]modelEntry, error) {
	if strings.TrimSpace(value.AccessToken) == "" {
		return nil, ErrMissingAccessToken
	}
	entries, err := fetchModelCatalog(ctx, profile, value, baseURL)
	if err == nil && len(entries) > 0 {
		return entries, nil
	}
	// 国际版天然不支持动态目录；国内版在动态失败时同样回落到快照，
	// 保证新建分组时始终有可选模型。
	if fallback := fallbackCatalog[profile.Key]; len(fallback) > 0 {
		return append([]modelEntry(nil), fallback...), nil
	}
	if err != nil {
		return nil, err
	}
	return nil, errors.New("Trae model catalog was empty")
}

func fetchModelCatalog(ctx context.Context, profile Profile, value Credential, baseURL string) ([]modelEntry, error) {
	if !profile.ModelListSupported {
		return nil, &UpstreamHTTPError{Operation: "model_list", StatusCode: http.StatusForbidden}
	}
	base := firstNonEmpty(baseURL, value.EffectiveBaseURL(profile))
	// CN 的 llm_raw_chat 只认 batch_get_detail_param 里的配置名，model_list
	// 的条目（如 seed_m8）发过去会被判 4001，因此优先用真实目录。
	if profile.Key == realmCN {
		if entries, err := detailParamModels(ctx, profile, value); err == nil && len(entries) > 0 {
			return entries, nil
		}
	}
	body, status, err := coreJSON(ctx, profile, value, base+profile.ModelListPath, map[string]any{"type": "chat"})
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &UpstreamHTTPError{Operation: "model_list", StatusCode: status}
	}
	var payload struct {
		Models       []map[string]any `json:"models"`
		ModelConfigs []map[string]any `json:"model_configs"`
		Data         struct {
			Models       []map[string]any `json:"models"`
			ModelConfigs []map[string]any `json:"model_configs"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("parse Trae model catalog: %w", err)
	}
	items := payload.ModelConfigs
	if len(items) == 0 {
		items = payload.Models
	}
	if len(items) == 0 {
		items = payload.Data.ModelConfigs
	}
	if len(items) == 0 {
		items = payload.Data.Models
	}
	result := make([]modelEntry, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		id := firstNonEmpty(
			stringValue(item["model_name"]), stringValue(item["modelName"]),
			stringValue(item["id"]), stringValue(item["name"]), stringValue(item["model"]),
		)
		if id == "" {
			continue
		}
		if truthy(item["disable"]) || truthy(item["disabled"]) || truthy(item["is_disable"]) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		name := firstNonEmpty(stringValue(item["display_name"]), stringValue(item["displayName"]), stringValue(item["title"]), id)
		result = append(result, modelEntry{ID: id, Name: name})
	}
	return result, nil
}

// modelDiscovery 把模型目录适配到订阅运行时。
type modelDiscovery struct{ profile Profile }

func (d modelDiscovery) ID() spec.UtilityID { return d.profile.ModelsID }

func (d modelDiscovery) DiscoverModels(ctx context.Context, credential subscriptionruntime.Credential, target subscriptionruntime.Target) ([]string, error) {
	value, err := ParseCredentialJSON(credential.Canonical(), d.profile)
	if err != nil {
		return nil, err
	}
	baseURL, err := target.BaseURL()
	if err != nil {
		return nil, err
	}
	models, err := ListModels(ctx, d.profile, value, baseURL)
	if err != nil {
		var upstream *UpstreamHTTPError
		if errors.As(err, &upstream) {
			return nil, &subscriptionruntime.UpstreamHTTPError{StatusCode: upstream.StatusCode}
		}
		return nil, err
	}
	return models, nil
}

func (d modelDiscovery) DiscoverNamedModels(ctx context.Context, credential subscriptionruntime.Credential, target subscriptionruntime.Target) ([]subscriptionruntime.ModelDescriptor, error) {
	value, err := ParseCredentialJSON(credential.Canonical(), d.profile)
	if err != nil {
		return nil, err
	}
	baseURL, err := target.BaseURL()
	if err != nil {
		return nil, err
	}
	entries, err := ListNamedModels(ctx, d.profile, value, baseURL)
	if err != nil {
		var upstream *UpstreamHTTPError
		if errors.As(err, &upstream) {
			return nil, &subscriptionruntime.UpstreamHTTPError{StatusCode: upstream.StatusCode}
		}
		return nil, err
	}
	result := make([]subscriptionruntime.ModelDescriptor, 0, len(entries))
	for _, entry := range entries {
		result = append(result, subscriptionruntime.ModelDescriptor{ID: entry.ID, Name: entry.Name})
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// 公共 HTTP 辅助
// ---------------------------------------------------------------------------

// coreJSON 向核心网关发一次鉴权 JSON 请求。
func coreJSON(ctx context.Context, profile Profile, value Credential, endpoint string, body map[string]any) ([]byte, int, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, 0, err
	}
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	if client == nil {
		client = &http.Client{}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return nil, 0, err
	}
	applyHeaders(request.Header, chatHeaders(profile, value, ""))
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, maxChatResponseBytes))
	return raw, response.StatusCode, nil
}

func truthy(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case float64:
		return typed != 0
	case string:
		return typed == "true" || typed == "1"
	default:
		return false
	}
}
