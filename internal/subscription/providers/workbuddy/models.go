package workbuddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"gpt-load/internal/channel/modules"
	"gpt-load/internal/channel/spec"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

// UpstreamHTTPError classifies a model-catalog failure without exposing bodies.
type UpstreamHTTPError struct {
	Operation  string
	StatusCode int
}

func (err *UpstreamHTTPError) Error() string {
	return fmt.Sprintf("WorkBuddy %s returned status %d", err.Operation, err.StatusCode)
}

// ListModels discovers the account's model catalog.
func ListModels(ctx context.Context, value Credential, baseURL string) ([]string, error) {
	entries, err := ListNamedModels(ctx, value, baseURL)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.ID)
	}
	return result, nil
}

// ListNamedModels discovers the catalog with the readable name upstream
// advertises next to each callable model id.
func ListNamedModels(ctx context.Context, value Credential, baseURL string) ([]modelEntry, error) {
	realm := realmFor(value.Realm, firstNonEmpty(baseURL, value.BaseURL), value.Domain)
	base := normalizeBaseURL(firstNonEmpty(baseURL, value.BaseURL), realm)
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{}
	}
	// The two catalogs are not supersets of each other: /v3/config carries the
	// account-scoped CLI list while the console catalog carries context-window
	// metadata and ids the config omits. Merge instead of falling back.
	configEntries, configErr := listConfigModels(ctx, client, base, realm, value.AccessToken)
	enterpriseEntries, enterpriseErr := listEnterpriseModels(ctx, client, base, realm, value.AccessToken)
	merged := mergeCatalogEntries(configEntries, enterpriseEntries)
	if len(merged) > 0 {
		return merged, nil
	}
	if configErr != nil {
		return nil, configErr
	}
	if enterpriseErr != nil {
		return nil, enterpriseErr
	}
	return nil, errors.New("WorkBuddy model catalog was empty")
}

// modelEntry pairs a callable model id with its upstream display name.
type modelEntry struct {
	ID   string
	Name string
}

type catalogModel struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Vendor          string   `json:"vendor"`
	MaxOutputTokens int      `json:"maxOutputTokens"`
	Tags            []string `json:"tags"`
	Disabled        bool     `json:"disabled"`
	IsDefault       bool     `json:"isDefault"`
	SupportsTool    *bool    `json:"supportsToolCall"`
	IsFree          *bool    `json:"isFree"`
}

type configPayload struct {
	Code json.RawMessage `json:"code"`
	Data struct {
		Models []catalogModel `json:"models"`
		Agents []struct {
			Name   string          `json:"name"`
			Models json.RawMessage `json:"models"`
		} `json:"agents"`
	} `json:"data"`
}

func listConfigModels(ctx context.Context, client *http.Client, base, realm, accessToken string) ([]modelEntry, error) {
	body, status, err := catalogRequest(ctx, client, base+"/v3/config", configUserAgent(realm), realm, accessToken, "config")
	if err != nil {
		return nil, err
	}
	var payload configPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("parse WorkBuddy config: %w", err)
	}
	if code := codeText(payload.Code); code != "" && code != "0" {
		return nil, &UpstreamHTTPError{Operation: "config", StatusCode: httpStatusForCode(code, status)}
	}
	// WorkBuddy only returns the CLI catalog under its CLI user agent, and it
	// nests it inside the "cli" agent. When that list is present it is the
	// authoritative set of callable ids; data.models only enriches it.
	allowed := allowedModelIDs(payload.Data.Agents)
	return catalogEntries(payload.Data.Models, allowed), nil
}

func listEnterpriseModels(ctx context.Context, client *http.Client, base, realm, accessToken string) ([]modelEntry, error) {
	body, status, err := catalogRequest(ctx, client, base+"/console/enterprises/personal/models", desktopUserAgent(realm), realm, accessToken, "models")
	if err != nil {
		return nil, err
	}
	var payload configPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("parse WorkBuddy models: %w", err)
	}
	if code := codeText(payload.Code); code != "" && code != "0" {
		return nil, &UpstreamHTTPError{Operation: "models", StatusCode: httpStatusForCode(code, status)}
	}
	allowed := allowedModelIDs(payload.Data.Agents)
	return catalogEntries(payload.Data.Models, allowed), nil
}

// allowedModelIDs returns the ids listed by the CLI agent, or nil when the
// response does not expose an agent-scoped list.
func allowedModelIDs(agents []struct {
	Name   string          `json:"name"`
	Models json.RawMessage `json:"models"`
}) map[string]struct{} {
	allowed := make(map[string]struct{})
	for _, agent := range agents {
		if !strings.EqualFold(strings.TrimSpace(agent.Name), "cli") {
			continue
		}
		for _, id := range decodeAgentModels(agent.Models) {
			allowed[id] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil
	}
	return allowed
}

func catalogEntries(models []catalogModel, allowed map[string]struct{}) []modelEntry {
	result := make([]modelEntry, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		id := strings.TrimSpace(model.ID)
		if id == "" || model.Disabled {
			continue
		}
		if allowed != nil {
			if _, ok := allowed[id]; !ok {
				continue
			}
		}
		if !chatCapableModel(model) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		name := strings.TrimSpace(model.Name)
		if name == "" {
			name = id
		}
		result = append(result, modelEntry{ID: id, Name: name})
	}
	return result
}

func mergeCatalogEntries(groups ...[]modelEntry) []modelEntry {
	result := make([]modelEntry, 0)
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, entry := range group {
			if _, ok := seen[entry.ID]; ok {
				continue
			}
			seen[entry.ID] = struct{}{}
			result = append(result, entry)
		}
	}
	return result
}

func decodeAgentModels(raw json.RawMessage) []string {
	var names []string
	if err := json.Unmarshal(raw, &names); err == nil {
		result := make([]string, 0, len(names))
		for _, name := range names {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		return result
	}
	var objects []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &objects); err != nil {
		return nil
	}
	result := make([]string, 0, len(objects))
	for _, object := range objects {
		if id := strings.TrimSpace(object.ID); id != "" {
			result = append(result, id)
		}
	}
	return result
}

// routingAliases are IDE-side model selectors, not callable CLI model ids.
var routingAliases = map[string]struct{}{
	"auto": {}, "auto-chat": {}, "default-model": {}, "fast-model": {},
	"balanced-model": {}, "primary-model": {},
}

func chatCapableModel(model catalogModel) bool {
	if model.IsDefault {
		return false
	}
	if model.MaxOutputTokens > 0 && model.MaxOutputTokens <= 256 {
		return false
	}
	for _, tag := range model.Tags {
		if strings.EqualFold(strings.TrimSpace(tag), "text-to-image") {
			return false
		}
	}
	id := strings.ToLower(strings.TrimSpace(model.ID))
	if _, ok := routingAliases[id]; ok {
		return false
	}
	for _, prefix := range []string{"nes-", "completion-", "enhance-", "codewise-"} {
		if strings.HasPrefix(id, prefix) {
			return false
		}
	}
	return true
}

func catalogRequest(ctx context.Context, client *http.Client, endpoint, userAgent, realm, accessToken, operation string) ([]byte, int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	headers := chatBaseHeaders(realm)
	headers.Set("User-Agent", userAgent)
	if accessToken != "" {
		headers.Set("Authorization", "Bearer "+accessToken)
	}
	applyHeaders(request.Header, headers)
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, response.StatusCode, &UpstreamHTTPError{Operation: operation, StatusCode: response.StatusCode}
	}
	return body, response.StatusCode, nil
}

// configUserAgent selects the catalog WorkBuddy returns. The global host only
// exposes the CLI agent (and therefore the CLI models) to the WorkBuddy CLI UA;
// an IDE UA yields the IDE configuration whose entries are routing aliases.
func configUserAgent(realm string) string {
	if realm == realmCN {
		return cliUserAgent
	}
	return "CLI/2.0.0 WorkBuddy/5.5.6"
}

// workbuddyModelDiscovery adapts ListModels to the subscription runtime.
type workbuddyModelDiscovery struct{}

func (workbuddyModelDiscovery) ID() spec.UtilityID { return modules.WorkBuddyModelDiscovery }

func (workbuddyModelDiscovery) DiscoverModels(ctx context.Context, credential subscriptionruntime.Credential, target subscriptionruntime.Target) ([]string, error) {
	value, err := ParseCredentialJSON(credential.Canonical())
	if err != nil {
		return nil, err
	}
	baseURL, err := target.BaseURL()
	if err != nil {
		return nil, err
	}
	models, err := ListModels(ctx, value, baseURL)
	if err != nil {
		var upstream *UpstreamHTTPError
		if errors.As(err, &upstream) {
			return nil, &subscriptionruntime.UpstreamHTTPError{StatusCode: upstream.StatusCode}
		}
		return nil, err
	}
	return models, nil
}

// DiscoverNamedModels exposes display names to the discovery dialog.
func (workbuddyModelDiscovery) DiscoverNamedModels(ctx context.Context, credential subscriptionruntime.Credential, target subscriptionruntime.Target) ([]subscriptionruntime.ModelDescriptor, error) {
	value, err := ParseCredentialJSON(credential.Canonical())
	if err != nil {
		return nil, err
	}
	baseURL, err := target.BaseURL()
	if err != nil {
		return nil, err
	}
	entries, err := ListNamedModels(ctx, value, baseURL)
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func chatBaseHeaders(realm string) http.Header {
	headers := make(http.Header)
	headers.Set("Accept", "application/json")
	headers.Set("Accept-Language", languageFor(realm))
	headers.Set("X-Requested-With", "XMLHttpRequest")
	headers.Set("X-CodeBuddy-Request", "1")
	headers.Set("Origin", originFor(realm))
	headers.Set("Referer", originFor(realm)+"/")
	return headers
}

// HTTPStatusForCode is exported for the bridge's error classification.
func HTTPStatusForCode(code string) int { return httpStatusForCode(code, 0) }

func httpStatusForCode(code string, fallback int) int {
	switch normalizeErrorCode(code) {
	case "12153":
		return http.StatusUnauthorized
	case "11140", "11128":
		return http.StatusForbidden
	case "11101", "11102", "11115", "11133", "11135", "11158":
		return http.StatusBadRequest
	case "14017", "14018", "6004":
		return http.StatusTooManyRequests
	case "6021", "11134":
		return http.StatusServiceUnavailable
	case "", "0":
		return fallback
	default:
		if fallback != 0 {
			return fallback
		}
		return http.StatusBadRequest
	}
}
