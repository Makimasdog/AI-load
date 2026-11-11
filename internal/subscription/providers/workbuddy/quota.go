package workbuddy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gpt-load/internal/channel/modules"
	"gpt-load/internal/channel/spec"
	"gpt-load/internal/subscription/providers/observation"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	// billing paths live at the API root (no /v2) except the plan lookup.
	billingResourceSummary = "/billing/meter/get-user-resource-summary"
	billingFreePackages    = "/billing/meter/get-user-resource-free-packages"
	billingPaymentType     = "/v2/billing/meter/get-payment-type"

	quotaCacheTTL = 60 * time.Second
	quotaTimeout  = 10 * time.Second

	capacityTypeSlice = 4
)

// freePackageCodes is the set api3 accepts; sending anything else is rejected
// with 400 / 10001 "PackageCodes required".
var freePackageCodes = []string{
	"TCACA_code_001_PqouKr6QWV",
	"TCACA_code_008_cfWoLwvjU4",
	"TCACA_code_035_ArVxJcGDsm",
	"TCACA_code_006_DbXS0lrypC",
	"TCACA_code_039_KRcQj7wUat",
	"TCACA_code_040_mi9rCYg46x",
	"TCACA_code_007_nzdH5h4Nl0",
	"TCACA_code_028_NtpWi0jzXs",
	"TCACA_code_037_WxOD3MpI2o",
	"TCACA_code_029_6wCGEWquYy",
	"TCACA_code_030_BjSt89qTvr",
}

var packageDisplayNames = map[string]string{
	"TCACA_code_001_PqouKr6QWV": "免费版",
	"TCACA_code_002_AkiJS3ZHF5": "Pro 月包",
	"TCACA_code_003_FAnt7lcmRT": "Pro 年包",
	"TCACA_code_005_maRGyrHhw1": "Pro 月包 Plus",
	"TCACA_code_006_DbXS0lrypC": "赠送包（Pro 试用）",
	"TCACA_code_007_nzdH5h4Nl0": "运营裂变包",
	"TCACA_code_008_cfWoLwvjU4": "体验版（周期刷新）",
	"TCACA_code_009_0XmEQc2xOf": "加量包",
	"TCACA_code_023_4xbGhMrE6q": "青春版",
	"TCACA_code_026_BaESVICNoi": "高级版",
	"TCACA_code_027_0FCGVA6vSa": "旗舰版",
	"TCACA_code_028_NtpWi0jzXs": "赠送包 28",
	"TCACA_code_029_6wCGEWquYy": "赠送包 29",
	"TCACA_code_030_BjSt89qTvr": "赠送包 30",
	"TCACA_code_035_ArVxJcGDsm": "Free Plan（周期刷新）",
	"TCACA_code_036_lupO5WgNdG": "加量包（国际）",
	"TCACA_code_037_WxOD3MpI2o": "赠送包（国际）",
	"TCACA_code_038_OhvqZtiPKr": "加量包",
	"TCACA_code_039_KRcQj7wUat": "Pro 试用（月）",
	"TCACA_code_040_mi9rCYg46x": "Pro 试用（年）",
}

type quotaCacheEntry struct {
	snapshot  observation.Snapshot
	fetchedAt time.Time
}

var quotaCache = struct {
	sync.Mutex
	entries map[string]quotaCacheEntry
}{entries: map[string]quotaCacheEntry{}}

type workbuddyQuotaObservation struct{}

func (workbuddyQuotaObservation) ID() spec.UtilityID { return modules.WorkBuddyQuotaObservation }

func (workbuddyQuotaObservation) Observe(ctx context.Context, credential subscriptionruntime.Credential, target subscriptionruntime.Target) (subscriptionruntime.Observation, error) {
	value, err := ParseCredentialJSON(credential.Canonical())
	if err != nil {
		return subscriptionruntime.Observation{}, err
	}
	if baseURL, baseErr := target.BaseURL(); baseErr == nil && strings.TrimSpace(baseURL) != "" {
		value.BaseURL = baseURL
	}
	snapshot, err := observedQuotaSnapshot(ctx, value)
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

func observedQuotaSnapshot(ctx context.Context, value Credential) (observation.Snapshot, error) {
	cacheKey := strings.TrimSpace(value.UID)
	if cacheKey == "" {
		cacheKey = value.AccessToken
	}
	quotaCache.Lock()
	if entry, ok := quotaCache.entries[cacheKey]; ok && time.Since(entry.fetchedAt) < quotaCacheTTL {
		snapshot := entry.snapshot
		quotaCache.Unlock()
		return snapshot, nil
	}
	quotaCache.Unlock()

	snapshot, err := fetchQuotaSnapshot(ctx, value)
	if err != nil {
		return observation.Snapshot{}, err
	}
	quotaCache.Lock()
	quotaCache.entries[cacheKey] = quotaCacheEntry{snapshot: snapshot, fetchedAt: time.Now()}
	quotaCache.Unlock()
	return snapshot, nil
}

func fetchQuotaSnapshot(ctx context.Context, value Credential) (observation.Snapshot, error) {
	realm := realmFor(value.Realm, value.BaseURL, value.Domain)
	base := normalizeBaseURL(value.BaseURL, realm)
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return observation.Snapshot{}, err
	}
	if client == nil {
		client = &http.Client{}
	}
	data, status, err := billingPost(ctx, client, base, realm, value, billingResourceSummary, []byte("{}"))
	if err != nil {
		return observation.Snapshot{}, billingError(err, status)
	}
	var summary struct {
		Packages   []map[string]any `json:"Packages"`
		IsPaidUser bool             `json:"IsPaidUser"`
	}
	if err := json.Unmarshal(data, &summary); err != nil {
		return observation.Snapshot{}, fmt.Errorf("parse WorkBuddy quota summary: %w", err)
	}
	facts := freePackageFacts(ctx, client, base, realm, value)
	snapshot := observation.Snapshot{
		Plan:         observation.PlanSummary{},
		Account:      &observation.AccountSummary{DisplayName: value.DisplayLabel(), Email: value.Email},
		QuotaWindows: make([]observation.QuotaWindow, 0, len(summary.Packages)),
	}
	creditTotal := 0.0
	for _, pkg := range summary.Packages {
		code := stringValue(pkg["PackageCode"])
		if code == "" {
			continue
		}
		left := numericValue(pkg["CycleRemainCapacity"])
		total := numericValue(pkg["CycleTotalCapacity"])
		fact := facts[code]
		label := strings.TrimSpace(fact.name)
		if label == "" {
			label = packageDisplayNames[code]
		}
		if label == "" {
			label = code
		}
		resetAt := cycleEndMillis(fact.cycleEnd)
		state := quotaWindowState(total, left)
		window := observation.QuotaWindow{
			ID: "package:" + code, Label: label, Scope: "account", Unit: "credits",
			Limit: floatPtr(total), Remaining: floatPtr(left), State: state, IsPrimary: fact.refills,
		}
		if total >= left {
			window.Used = floatPtr(total - left)
		}
		if total > 0 {
			window.Utilization = floatPtr((total - left) / total)
		}
		if resetAt != nil {
			window.ResetAtMS = resetAt
		}
		snapshot.QuotaWindows = append(snapshot.QuotaWindows, window)
		creditTotal += left
	}
	sort.SliceStable(snapshot.QuotaWindows, func(i, j int) bool {
		if snapshot.QuotaWindows[i].IsPrimary != snapshot.QuotaWindows[j].IsPrimary {
			return snapshot.QuotaWindows[i].IsPrimary
		}
		return snapshot.QuotaWindows[i].Label < snapshot.QuotaWindows[j].Label
	})
	hasCredits := creditTotal > 0
	snapshot.Credits = &observation.CreditSummary{
		Balance: formatCredits(creditTotal), HasCredits: boolPtr(hasCredits),
		ObservedAtMS: int64Ptr(time.Now().UnixMilli()),
	}
	snapshot.Plan = observation.PlanSummary{Name: planName(ctx, client, base, realm, value, summary.IsPaidUser)}
	snapshot.Plan.Level = planLevel(snapshot.Plan.Name, summary.IsPaidUser)
	return snapshot, nil
}

type packageFacts struct {
	name     string
	cycleEnd any
	refills  bool
}

// quotaWindowState maps observed capacity to the UI contract, which only
// accepts "available", "exhausted" or "unknown".
func quotaWindowState(total, left float64) string {
	if total > 0 && left <= 0 {
		return "exhausted"
	}
	return "available"
}

func freePackageFacts(ctx context.Context, client *http.Client, base, realm string, value Credential) map[string]packageFacts {
	facts := map[string]packageFacts{}
	data, _, err := billingPost(ctx, client, base, realm, value, billingFreePackages, freePackagesRequestBody())
	if err != nil {
		return facts
	}
	var payload struct {
		Accounts []map[string]any `json:"Accounts"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return facts
	}
	for _, pkg := range payload.Accounts {
		code := stringValue(pkg["PackageCode"])
		if code == "" {
			continue
		}
		fact := facts[code]
		if name := stringValue(pkg["PackageName"]); name != "" {
			fact.name = name
		}
		if end := pkg["CycleEndTime"]; end != nil {
			fact.cycleEnd = end
		}
		if numericValue(pkg["CapacityType"]) == capacityTypeSlice {
			fact.refills = true
		}
		facts[code] = fact
	}
	return facts
}

func freePackagesRequestBody() []byte {
	const layout = "2006-01-02 15:04:05"
	now := time.Now()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	end := start.AddDate(0, 0, 1).Add(-time.Second)
	body, err := json.Marshal(map[string]any{
		"PageNumber": 1, "PageSize": 100, "PackageCodes": freePackageCodes, "Status": []int{0, 3},
		"SlicePeriodStartTime": start.Format(layout), "SlicePeriodEndTime": end.Format(layout),
	})
	if err != nil {
		return []byte("{}")
	}
	return body
}

func planName(ctx context.Context, client *http.Client, base, realm string, value Credential, paid bool) string {
	data, _, err := billingPost(ctx, client, base, realm, value, billingPaymentType, []byte("{}"))
	if err == nil {
		var payload struct {
			PaymentType string `json:"paymentType"`
		}
		if json.Unmarshal(data, &payload) == nil && strings.TrimSpace(payload.PaymentType) != "" {
			return strings.TrimSpace(payload.PaymentType)
		}
	}
	if paid {
		return "paid"
	}
	return "free"
}

func planLevel(name string, paid bool) observation.PlanLevel {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "elite"), strings.Contains(lower, "旗舰"):
		return observation.PlanLevelElite
	case strings.Contains(lower, "premium"), strings.Contains(lower, "高级"), strings.Contains(lower, "pro"):
		return observation.PlanLevelPremium
	case paid:
		return observation.PlanLevelStandard
	default:
		return observation.PlanLevelFree
	}
}

func billingPost(ctx context.Context, client *http.Client, base, realm string, value Credential, path string, body []byte) (json.RawMessage, int, error) {
	endpoint := base + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	headers := chatBaseHeaders(realm)
	headers.Set("Content-Type", "application/json")
	headers.Set("User-Agent", quotaUserAgent(realm))
	if strings.TrimSpace(value.AccessToken) != "" {
		headers.Set("Authorization", "Bearer "+value.AccessToken)
	}
	if uid := strings.TrimSpace(value.UID); uid != "" {
		headers.Set("X-User-Id", uid)
	}
	if domain := strings.TrimSpace(value.Domain); domain != "" {
		headers.Set("X-Domain", domain)
	}
	applyHeaders(request.Header, headers)
	timeoutClient := *client
	timeoutClient.Timeout = quotaTimeout
	response, err := timeoutClient.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	envelope := decodeAuthEnvelope(raw)
	if response.StatusCode < 200 || response.StatusCode >= 300 || !envelope.ok() {
		return nil, response.StatusCode, &TokenEndpointError{
			Status: response.StatusCode, Code: envelope.codeText(), Message: envelope.Msg,
		}
	}
	return envelope.Data, response.StatusCode, nil
}

func quotaUserAgent(realm string) string {
	if realm == realmCN {
		return cliUserAgent
	}
	return "CLI/2.0.0 WorkBuddy/5.5.6"
}

func billingError(err error, status int) error {
	if status != 0 {
		return &subscriptionruntime.UpstreamHTTPError{StatusCode: status}
	}
	return err
}

func formatCredits(value float64) string {
	if value == 0 {
		return "0"
	}
	rendered := strconv.FormatFloat(value, 'f', 2, 64)
	rendered = strings.TrimRight(rendered, "0")
	return strings.TrimRight(rendered, ".")
}

func numericValue(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return parsed
	case json.Number:
		parsed, _ := t.Float64()
		return parsed
	default:
		return 0
	}
}

func cycleEndMillis(v any) *int64 {
	var ms int64
	switch t := v.(type) {
	case float64:
		ms = int64(t)
	case json.Number:
		parsed, _ := t.Int64()
		ms = parsed
	case string:
		trimmed := strings.TrimSpace(t)
		if trimmed == "" {
			return nil
		}
		if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			ms = parsed
		} else {
			for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339} {
				if parsed, err := time.ParseInLocation(layout, trimmed, time.Local); err == nil {
					ms = parsed.UnixMilli()
					break
				}
			}
		}
	}
	if ms < 1e12 {
		return nil
	}
	return &ms
}

func floatPtr(value float64) *float64 { return &value }
func boolPtr(value bool) *bool        { return &value }
func int64Ptr(value int64) *int64     { return &value }
