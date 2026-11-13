package trae

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gpt-load/internal/channel/spec"
	"gpt-load/internal/subscription/providers/observation"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	quotaCacheTTL = 60 * time.Second
	quotaTimeout  = 12 * time.Second
)

type quotaCacheEntry struct {
	snapshot  observation.Snapshot
	fetchedAt time.Time
}

var quotaCache = struct {
	sync.Mutex
	entries map[string]quotaCacheEntry
}{entries: map[string]quotaCacheEntry{}}

// quotaObservation 适配额度观测能力。
type quotaObservation struct{ profile Profile }

func (o quotaObservation) ID() spec.UtilityID { return o.profile.QuotaID }

func (o quotaObservation) Observe(ctx context.Context, credential subscriptionruntime.Credential, target subscriptionruntime.Target) (subscriptionruntime.Observation, error) {
	value, err := ParseCredentialJSON(credential.Canonical(), o.profile)
	if err != nil {
		return subscriptionruntime.Observation{}, err
	}
	if baseURL, baseErr := target.BaseURL(); baseErr == nil && strings.TrimSpace(baseURL) != "" {
		value.BaseURL = baseURL
	}
	snapshot, err := observedQuotaSnapshot(ctx, o.profile, value)
	if err != nil {
		return subscriptionruntime.Observation{}, err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return subscriptionruntime.Observation{}, err
	}
	return subscriptionruntime.Observation{
		Payload: payload, AccountObserved: true, QuotaObserved: true,
		ObservedQuotaScopes: []string{"account"},
	}, nil
}

func observedQuotaSnapshot(ctx context.Context, profile Profile, value Credential) (observation.Snapshot, error) {
	cacheKey := profile.Key + ":" + strings.TrimSpace(value.UID)
	if strings.TrimSpace(value.UID) == "" {
		cacheKey = profile.Key + ":token:" + shortHash(value.AccessToken)
	}
	quotaCache.Lock()
	if entry, ok := quotaCache.entries[cacheKey]; ok && time.Since(entry.fetchedAt) < quotaCacheTTL {
		snapshot := entry.snapshot
		quotaCache.Unlock()
		return snapshot, nil
	}
	quotaCache.Unlock()

	snapshot, err := fetchQuotaSnapshot(ctx, profile, value)
	if err != nil {
		return observation.Snapshot{}, err
	}
	quotaCache.Lock()
	quotaCache.entries[cacheKey] = quotaCacheEntry{snapshot: snapshot, fetchedAt: time.Now()}
	quotaCache.Unlock()
	return snapshot, nil
}

func fetchQuotaSnapshot(ctx context.Context, profile Profile, value Credential) (observation.Snapshot, error) {
	if strings.TrimSpace(value.AccessToken) == "" {
		return observation.Snapshot{}, ErrMissingAccessToken
	}
	payHost := trimBaseURL(firstNonEmpty(profile.PayURL, profile.AccountURL))
	entBody, status, err := payJSON(ctx, profile, value, payHost+profile.EntUsagePath)
	if err != nil || status < 200 || status >= 300 {
		// 备选主机：国际版 pay 与 account 不是同一台。
		if fallback := strings.TrimSpace(profile.PayFallback); fallback != "" {
			entBody, status, err = payJSON(ctx, profile, value, trimBaseURL(fallback)+profile.EntUsagePath)
		}
	}
	if err != nil {
		return observation.Snapshot{}, err
	}
	if status < 200 || status >= 300 {
		return observation.Snapshot{}, &subscriptionruntime.UpstreamHTTPError{StatusCode: status}
	}
	result := decodeResult(entBody)
	packs := entitlementPacks(result)
	// CN 的 v2 ent_usage 把账号级总量放在 usage_summary 里，权益包只带
	// entitlement_base_info.charge_amount（面额，不表示剩余）；此前只读
	// 包上的 quota/usage 字段，导致整行额度恒为 0。
	totalAmount, consumedAmount, hasSummary := usageSummary(result)
	slog.Info("trae quota usage summary", "realm", profile.Key,
		"has_summary", hasSummary, "total_amount", totalAmount,
		"consumed_amount", consumedAmount, "packs", len(packs))
	snapshot := observation.Snapshot{
		Account:      &observation.AccountSummary{DisplayName: value.DisplayLabel(), Email: value.Email},
		QuotaWindows: make([]observation.QuotaWindow, 0, len(packs)+1),
	}
	totalRemaining := 0.0
	if hasSummary {
		remaining := totalAmount - consumedAmount
		if remaining < 0 {
			remaining = 0
		}
		window := observation.QuotaWindow{
			ID: "credits:total", Label: "总积分", Scope: "account", Unit: "credits",
			Limit: floatPtr(totalAmount), Used: floatPtr(consumedAmount),
			Remaining: floatPtr(remaining), State: quotaState(totalAmount, remaining),
			IsPrimary: true,
		}
		if totalAmount > 0 {
			window.Utilization = floatPtr(consumedAmount / totalAmount)
		}
		snapshot.QuotaWindows = append(snapshot.QuotaWindows, window)
		totalRemaining += remaining
	}
	for index, pack := range packs {
		faceOnly := false
		quota := numericFloat(pack["quota"])
		used := numericFloat(pack["usage"])
		if quota <= 0 {
			quota = numericFloat(pack["total_quota"])
		}
		if used <= 0 {
			used = numericFloat(pack["used"])
		}
		if quota <= 0 {
			// 新形态：只给面额，没有“该包剩余”，因此不臆造 Remaining。
			quota = packFaceValue(pack)
			faceOnly = quota > 0
		}
		remaining := quota - used
		if remaining < 0 {
			remaining = 0
		}
		// 账号级总量已经由 usage_summary 给出时，只保留带真实数量的包；
		// 只有 display_desc 的空包不再渲染成一排 0。
		if quota <= 0 && !faceOnly && hasSummary {
			continue
		}
		label := firstNonEmpty(
			stringValue(pack["display_desc"]), stringValue(pack["pack_name"]),
			stringValue(pack["display_name"]), stringValue(pack["name"]),
			stringValue(pack["entitlement_pack_name"]), fmt.Sprintf("权益包 %d", index+1),
		)
		id := firstNonEmpty(stringValue(pack["pack_id"]), stringValue(pack["entitlement_pack_id"]), label)
		window := observation.QuotaWindow{
			ID: "pack:" + id, Label: label, Scope: "account", Unit: "credits",
			State: quotaState(quota, remaining), IsPrimary: !hasSummary && index == 0,
		}
		if !faceOnly {
			window.Limit = floatPtr(quota)
			window.Used = floatPtr(used)
			window.Remaining = floatPtr(remaining)
			if quota > 0 {
				window.Utilization = floatPtr(used / quota)
			}
			totalRemaining += remaining
		} else if quota > 0 {
			// 只有面额：展示包的大小，但不声称剩余。
			window.Limit = floatPtr(quota)
		}
		if reset := packResetAt(pack); reset != nil {
			window.ResetAtMS = reset
		}
		snapshot.QuotaWindows = append(snapshot.QuotaWindows, window)
	}
	sort.SliceStable(snapshot.QuotaWindows, func(i, j int) bool {
		if snapshot.QuotaWindows[i].IsPrimary != snapshot.QuotaWindows[j].IsPrimary {
			return snapshot.QuotaWindows[i].IsPrimary
		}
		return snapshot.QuotaWindows[i].Label < snapshot.QuotaWindows[j].Label
	})
	hasCredits := totalRemaining > 0
	snapshot.Credits = &observation.CreditSummary{
		Balance: formatCredits(totalRemaining), HasCredits: boolPtr(hasCredits),
		ObservedAtMS: int64Ptr(time.Now().UnixMilli()),
	}
	planName, paid := payStatus(ctx, profile, value, payHost)
	if planName == "" {
		if paid {
			planName = "paid"
		} else {
			planName = "free"
		}
	}
	snapshot.Plan = observation.PlanSummary{Name: planName, Level: planLevel(planName, paid)}
	return snapshot, nil
}

func entitlementPacks(result map[string]any) []map[string]any {
	for _, key := range []string{"user_entitlement_pack_list", "userEntitlementPackList", "entitlement_pack_list"} {
		if items, ok := result[key].([]any); ok {
			return mapsOf(items)
		}
	}
	if data, ok := result["data"].(map[string]any); ok {
		return entitlementPacks(data)
	}
	return nil
}

func mapsOf(items []any) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if entry, ok := item.(map[string]any); ok {
			result = append(result, entry)
		}
	}
	return result
}

func quotaState(limit, remaining float64) string {
	if limit > 0 && remaining <= 0 {
		return "exhausted"
	}
	return "available"
}

func packResetAt(pack map[string]any) *int64 {
	for _, key := range []string{"expire_at", "expire_time", "expireAt", "cycle_end_time"} {
		if raw, ok := pack[key]; ok {
			if ms := normalizeExpiryMillis(raw); ms > 0 {
				return &ms
			}
		}
	}
	// v2 ent_usage 把有效期放在 entitlement_base_info.end_time。
	if base, ok := pack["entitlement_base_info"].(map[string]any); ok {
		for _, key := range []string{"end_time", "endTime", "expire_time"} {
			if raw, ok := base[key]; ok {
				if ms := normalizeExpiryMillis(raw); ms > 0 {
					return &ms
				}
			}
		}
	}
	return nil
}

// usageSummary 读取 v2 ent_usage 的账号级总量（CN 的额度只在这里）。
func usageSummary(result map[string]any) (limit, used float64, ok bool) {
	if data, isMap := result["data"].(map[string]any); isMap {
		if _, present := data["usage_summary"]; present {
			result = data
		}
	}
	summary, isMap := result["usage_summary"].(map[string]any)
	if !isMap {
		summary, isMap = result["usageSummary"].(map[string]any)
	}
	if !isMap {
		return 0, 0, false
	}
	total := numericFloat(summary["total_amount"])
	if total <= 0 {
		total = numericFloat(summary["totalAmount"])
	}
	consumed := numericFloat(summary["consumed_amount"])
	if consumed <= 0 {
		consumed = numericFloat(summary["consumedAmount"])
	}
	if total <= 0 && consumed <= 0 {
		return 0, 0, false
	}
	return total, consumed, true
}

// packFaceValue 读取权益包面额；它不代表剩余额度，只用于展示包本身的大小。
func packFaceValue(pack map[string]any) float64 {
	if value := numericFloat(pack["charge_amount"]); value > 0 {
		return value
	}
	if base, ok := pack["entitlement_base_info"].(map[string]any); ok {
		return numericFloat(base["charge_amount"])
	}
	return 0
}

func normalizeExpiryMillis(value any) int64 {
	switch typed := value.(type) {
	case float64:
		if typed > 1e12 {
			return int64(typed)
		}
		if typed > 1e9 {
			return int64(typed) * 1000
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return normalizeExpiryMillis(float64(parsed))
		}
		for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339} {
			if parsed, err := time.ParseInLocation(layout, trimmed, time.Local); err == nil {
				return parsed.UnixMilli()
			}
		}
	}
	return 0
}

func payStatus(ctx context.Context, profile Profile, value Credential, payHost string) (string, bool) {
	body, status, err := payJSON(ctx, profile, value, payHost+profile.PayStatusPath)
	if err != nil || status < 200 || status >= 300 {
		return "", false
	}
	result := decodeResult(body)
	name := firstNonEmpty(
		stringValue(result["plan_name"]), stringValue(result["product_type"]),
		stringValue(result["payment_type"]), stringValue(result["plan"]),
	)
	paid := truthy(result["is_paid_user"]) || truthy(result["is_paid"]) || truthy(result["is_subscribed"])
	if !paid && name != "" && !strings.EqualFold(name, "free") {
		paid = true
	}
	return name, paid
}

func payJSON(ctx context.Context, profile Profile, value Credential, endpoint string) ([]byte, int, error) {
	payload := []byte("{}")
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	if client == nil {
		client = &http.Client{}
	}
	request, err := newJSONRequest(ctx, endpoint, payload)
	if err != nil {
		return nil, 0, err
	}
	applyHeaders(request.Header, accountHeaders(profile, value))
	timeoutClient := *client
	timeoutClient.Timeout = quotaTimeout
	response, err := timeoutClient.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	return raw, response.StatusCode, nil
}

func planLevel(name string, paid bool) observation.PlanLevel {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "elite"), strings.Contains(lower, "旗舰"):
		return observation.PlanLevelElite
	case strings.Contains(lower, "premium"), strings.Contains(lower, "pro"), strings.Contains(lower, "高级"):
		return observation.PlanLevelPremium
	case paid:
		return observation.PlanLevelStandard
	default:
		return observation.PlanLevelFree
	}
}

func numericFloat(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case json.Number:
		parsed, _ := typed.Float64()
		return parsed
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed
	default:
		return 0
	}
}

func formatCredits(value float64) string {
	if value == 0 {
		return "0"
	}
	rendered := strconv.FormatFloat(value, 'f', 2, 64)
	rendered = strings.TrimRight(rendered, "0")
	return strings.TrimRight(rendered, ".")
}

func floatPtr(value float64) *float64 { return &value }
func boolPtr(value bool) *bool        { return &value }
func int64Ptr(value int64) *int64     { return &value }
