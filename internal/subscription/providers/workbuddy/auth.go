package workbuddy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	cliUserAgent           = "CLI/2.63.2 CodeBuddy/2.63.2"
	globalDesktopUserAgent = "WorkBuddy/5.5.4 WorkBuddy AI/5.5.4 CLI/2.137.1"
	cnDesktopUserAgent     = "WorkBuddy/5.5.4 WorkBuddy/5.5.4 CLI/2.137.1"
	ideUserAgent           = "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0"
	clientVersion          = "5.5.4"

	offlineSessionCode   = "12153"
	loginPendingCode     = "11217"
	refreshLeadTime      = 7 * 24 * time.Hour
	deviceAuthorization  = 10 * time.Minute
	devicePollInterval   = 2 * time.Second
	maxAuthResponseBytes = 1 << 20
)

type deviceState struct {
	State     string          `json:"state"`
	BaseURL   string          `json:"base_url"`
	Realm     string          `json:"realm"`
	ExpiresAt int64           `json:"expires_at"`
	Cookies   []sessionCookie `json:"cookies,omitempty"`
}

// sessionCookie 保存 auth/state 到 token 轮询之间需要保持的会话 Cookie。
// 上游按 tgw_l7_route 做节点粘滞，丢了 Cookie 后轮询可能落在别的节点上，
// 一直只能读到 "login ing..."。
type sessionCookie struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Path    string `json:"path,omitempty"`
	Secure  bool   `json:"secure,omitempty"`
	Expires int64  `json:"expires,omitempty"`
}

// TokenEndpointError describes a rejected login/refresh call.
type TokenEndpointError struct {
	Status  int
	Code    string
	Message string
	Retry   time.Duration
}

func (err *TokenEndpointError) Error() string {
	if err == nil {
		return ""
	}
	code := strings.TrimSpace(err.Code)
	switch {
	case code != "" && err.Status != 0:
		return fmt.Sprintf("WorkBuddy endpoint rejected the request (status=%d code=%s)", err.Status, code)
	case code != "":
		return fmt.Sprintf("WorkBuddy endpoint rejected the request (code=%s)", code)
	case err.Status != 0:
		return fmt.Sprintf("WorkBuddy endpoint rejected the request (status=%d)", err.Status)
	default:
		return "WorkBuddy endpoint rejected the request"
	}
}

func (err *TokenEndpointError) HTTPStatusCode() int { return err.Status }
func (err *TokenEndpointError) ErrorCode() string   { return err.Code }

func (err *TokenEndpointError) RetryAfter() *time.Duration {
	if err == nil || err.Retry <= 0 {
		return nil
	}
	value := err.Retry
	return &value
}

func (err *TokenEndpointError) NeedsReauthorization() bool {
	if err == nil {
		return false
	}
	return err.Status == http.StatusUnauthorized || strings.TrimSpace(err.Code) == offlineSessionCode
}

func authHTTPClient(ctx context.Context) (*http.Client, error) {
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{}
	}
	return client, nil
}

// loginHTTPClient 在基础客户端上挂一个 Cookie Jar，用于在发起授权与轮询之间
// 保持上游的会话/节点粘滞 Cookie。传入已有 Cookie 时会先恢复再使用。
func loginHTTPClient(ctx context.Context, base string, cookies []sessionCookie) (*http.Client, error) {
	baseClient, err := authHTTPClient(ctx)
	if err != nil {
		return nil, err
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	client := *baseClient
	client.Jar = jar
	if len(cookies) > 0 {
		if endpoint, parseErr := url.Parse(base); parseErr == nil && endpoint.Host != "" {
			jar.SetCookies(endpoint, toHTTPCookies(cookies))
		}
	}
	return &client, nil
}

func toHTTPCookies(cookies []sessionCookie) []*http.Cookie {
	result := make([]*http.Cookie, 0, len(cookies))
	for _, item := range cookies {
		if strings.TrimSpace(item.Name) == "" {
			continue
		}
		expires := time.Time{}
		if item.Expires > 0 {
			expires = time.Unix(item.Expires, 0)
		}
		result = append(result, &http.Cookie{
			Name: item.Name, Value: item.Value, Path: item.Path, Secure: item.Secure, Expires: expires,
		})
	}
	return result
}

func captureSessionCookies(client *http.Client, base string) []sessionCookie {
	if client == nil || client.Jar == nil {
		return nil
	}
	endpoint, err := url.Parse(base)
	if err != nil || endpoint.Host == "" {
		return nil
	}
	items := client.Jar.Cookies(endpoint)
	result := make([]sessionCookie, 0, len(items))
	for _, item := range items {
		cookie := sessionCookie{Name: item.Name, Value: item.Value, Path: item.Path, Secure: item.Secure}
		if !item.Expires.IsZero() {
			cookie.Expires = item.Expires.Unix()
		}
		result = append(result, cookie)
	}
	return result
}

func desktopUserAgent(realm string) string {
	if realm == realmCN {
		return cnDesktopUserAgent
	}
	return globalDesktopUserAgent
}

func originFor(realm string) string {
	if realm == realmCN {
		return cnOrigin
	}
	return globalOrigin
}

func BeginCredentialAuthorization(ctx context.Context) (subscriptionruntime.DeviceAuthorization, error) {
	base := defaultGlobalBaseURL
	client, err := loginHTTPClient(ctx, base, nil)
	if err != nil {
		return subscriptionruntime.DeviceAuthorization{}, err
	}
	response, err := postJSON(ctx, client, base+"/v2/plugin/auth/state?platform=CLI", authHeaders(base, realmGlobal, ""), []byte("{}"))
	if err != nil {
		return subscriptionruntime.DeviceAuthorization{}, err
	}
	if response.status < 200 || response.status >= 300 {
		return subscriptionruntime.DeviceAuthorization{}, &TokenEndpointError{Status: response.status, Code: response.envelopeCode(), Message: response.envelopeMessage()}
	}
	// 上游把登录态包在 {"code":0,"msg":"OK","data":{...}} 里；
	// 同时兼容 data 缺失时直接返回根对象的旧行为。
	envelope := decodeAuthEnvelope(response.body)
	if !envelope.ok() {
		return subscriptionruntime.DeviceAuthorization{}, &TokenEndpointError{
			Status: response.status, Code: envelope.codeText(), Message: envelope.Msg,
		}
	}
	var payload struct {
		State   string `json:"state"`
		AuthURL string `json:"authUrl"`
	}
	if len(response.body) == 0 {
		return subscriptionruntime.DeviceAuthorization{}, errors.New("WorkBuddy login state response was empty")
	}
	if err := json.Unmarshal(envelope.Data, &payload); err != nil {
		return subscriptionruntime.DeviceAuthorization{}, fmt.Errorf("parse WorkBuddy login state: %w", err)
	}
	if strings.TrimSpace(payload.State) == "" || strings.TrimSpace(payload.AuthURL) == "" {
		return subscriptionruntime.DeviceAuthorization{}, errors.New("WorkBuddy login state response was incomplete")
	}
	state, err := json.Marshal(deviceState{
		State: payload.State, BaseURL: base, Realm: realmGlobal,
		ExpiresAt: time.Now().Add(deviceAuthorization).Unix(),
		Cookies:   captureSessionCookies(client, base),
	})
	if err != nil {
		return subscriptionruntime.DeviceAuthorization{}, err
	}
	return subscriptionruntime.DeviceAuthorization{
		VerificationURL: payload.AuthURL, DriverState: state,
		ExpiresAt: time.Now().Add(deviceAuthorization), PollInterval: devicePollInterval,
	}, nil
}

func PollCredentialAuthorization(ctx context.Context, raw []byte) (subscriptionruntime.DeviceAuthorizationPoll, error) {
	var state deviceState
	if err := json.Unmarshal(raw, &state); err != nil {
		return subscriptionruntime.DeviceAuthorizationPoll{}, fmt.Errorf("parse WorkBuddy login state: %w", err)
	}
	if state.ExpiresAt > 0 && time.Now().Unix() > state.ExpiresAt {
		return subscriptionruntime.DeviceAuthorizationPoll{Status: subscriptionruntime.DeviceAuthorizationExpired, DriverState: raw}, nil
	}
	realm := realmFor(state.Realm, state.BaseURL, "")
	base := normalizeBaseURL(state.BaseURL, realm)
	client, err := loginHTTPClient(ctx, base, state.Cookies)
	if err != nil {
		return subscriptionruntime.DeviceAuthorizationPoll{}, err
	}
	endpoint := base + "/v2/plugin/auth/token?state=" + url.QueryEscape(state.State)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return subscriptionruntime.DeviceAuthorizationPoll{}, err
	}
	applyHeaders(request.Header, authHeaders(base, realm, ""))
	response, err := client.Do(request)
	// 上游可能在轮询期间轮换节点粘滞 Cookie，取回后随 DriverState 一起保存。
	state.Cookies = captureSessionCookies(client, base)
	raw = mustMarshalDeviceState(state)
	if err != nil {
		logrus.WithError(err).WithField("state", state.State).Warn("WorkBuddy login poll request failed")
		return pendingResult(raw), nil
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// 未完成授权时上游会返回非 2xx；继续轮询直到过期。
		logrus.WithFields(logrus.Fields{
			"state": state.State, "status": response.StatusCode, "body_bytes": len(body),
		}).Debug("WorkBuddy login poll not ready")
		return pendingResult(raw), nil
	}
	envelope := decodeAuthEnvelope(body)
	if !envelope.ok() {
		fields := logrus.Fields{"state": state.State, "status": response.StatusCode, "code": envelope.codeText(), "message": envelope.Msg}
		// 11217 是上游正常的"登录中"占位码，其余错误升级为 Warn，避免再次被静默吞掉。
		if envelope.codeText() != loginPendingCode {
			logrus.WithFields(fields).Warn("WorkBuddy login poll returned an unexpected status")
		} else {
			logrus.WithFields(fields).Debug("WorkBuddy login poll pending")
		}
		return pendingResult(raw), nil
	}
	var token tokenPayload
	if err := json.Unmarshal(envelope.Data, &token); err != nil {
		logrus.WithError(err).WithField("state", state.State).Warn("WorkBuddy login poll returned an unparseable token payload")
		return pendingResult(raw), nil
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		logrus.WithField("state", state.State).Warn("WorkBuddy login poll succeeded without an access token")
		return pendingResult(raw), nil
	}
	account, err := fetchAccount(ctx, client, base, realm, state.State, token.AccessToken)
	if err != nil {
		logrus.WithError(err).WithField("state", state.State).Warn("WorkBuddy login succeeded but the account lookup failed")
		return pendingResult(raw), nil
	}
	credential := Credential{
		Type: "workbuddy", Realm: realm, BaseURL: base,
		AccessToken: strings.TrimSpace(token.AccessToken), RefreshToken: strings.TrimSpace(token.RefreshToken),
		ExpiresAt: expiryFromSeconds(token.ExpiresIn, time.Now()),
		UID:       strings.TrimSpace(account.UID), EnterpriseID: strings.TrimSpace(account.EnterpriseID),
		Nickname: account.nickname(), Email: strings.TrimSpace(account.Email), Phone: account.phone(),
		Domain: strings.TrimSpace(token.Domain),
	}
	credential, err = normalizeCredential(credential)
	if err != nil {
		logrus.WithError(err).WithField("state", state.State).Warn("WorkBuddy login returned an unusable credential")
		return pendingResult(raw), nil
	}
	runtimeCredential, err := newRuntimeCredential(credential)
	if err != nil {
		return subscriptionruntime.DeviceAuthorizationPoll{}, err
	}
	return subscriptionruntime.DeviceAuthorizationPoll{
		Status: subscriptionruntime.DeviceAuthorizationAuthorized, Credential: runtimeCredential, DriverState: raw,
	}, nil
}

func pendingResult(raw []byte) subscriptionruntime.DeviceAuthorizationPoll {
	return subscriptionruntime.DeviceAuthorizationPoll{
		Status: subscriptionruntime.DeviceAuthorizationPending, DriverState: raw, PollInterval: devicePollInterval,
	}
}

func mustMarshalDeviceState(state deviceState) []byte {
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil
	}
	return encoded
}

func newRuntimeCredential(value Credential) (subscriptionruntime.Credential, error) {
	canonical, err := value.Marshal()
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	expiresAt, known := value.ExpiresAtTime()
	// Email is the framework's display slot for a subscription credential; the
	// UI shows it next to the account instead of an opaque credential id.
	account := subscriptionruntime.Account{Email: value.DisplayLabel(), ExpiresAt: expiresAt, ExpiresAtKnown: known}
	return subscriptionruntime.NewCredential(canonical, value.UID, account, expiresAt, known, value.SecretValues()), nil
}

func fetchAccount(ctx context.Context, client *http.Client, base, realm, state, accessToken string) (desktopAccount, error) {
	endpoint := base + "/v2/plugin/login/account?state=" + url.QueryEscape(state)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return desktopAccount{}, err
	}
	headers := authHeaders(base, realm, accessToken)
	applyHeaders(request.Header, headers)
	response, err := client.Do(request)
	if err != nil {
		return desktopAccount{}, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return desktopAccount{}, &TokenEndpointError{Status: response.StatusCode, Code: codeOf(body)}
	}
	envelope := decodeAuthEnvelope(body)
	if !envelope.ok() {
		return desktopAccount{}, &TokenEndpointError{Status: response.StatusCode, Code: envelope.codeText()}
	}
	account, err := decodeAccountPayload(envelope.Data)
	if err != nil {
		return desktopAccount{}, err
	}
	if strings.TrimSpace(account.UID) == "" {
		return desktopAccount{}, ErrCredentialIncomplete
	}
	return account, nil
}

// decodeAccountPayload accepts both shapes the upstream login surfaces expose:
// a bare account object and the browser landing payload {"accounts":[...]}.
func decodeAccountPayload(raw []byte) (desktopAccount, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return desktopAccount{}, ErrCredentialIncomplete
	}
	var account desktopAccount
	if err := json.Unmarshal(trimmed, &account); err == nil && strings.TrimSpace(account.UID) != "" {
		return account, nil
	}
	var wrapper struct {
		Accounts []desktopAccount `json:"accounts"`
	}
	if err := json.Unmarshal(trimmed, &wrapper); err != nil {
		return desktopAccount{}, err
	}
	if len(wrapper.Accounts) == 0 {
		return desktopAccount{}, ErrCredentialIncomplete
	}
	return wrapper.Accounts[0], nil
}

// RefreshCredentialOnce exchanges the refresh token for a new access token.
// Callers must serialize refreshes per credential (single-flight) and persist
// the returned value before using it.
func RefreshCredentialOnce(ctx context.Context, current Credential) (Credential, error) {
	realm := realmFor(current.Realm, current.BaseURL, current.Domain)
	base := normalizeBaseURL(current.BaseURL, realm)
	client, err := authHTTPClient(ctx)
	if err != nil {
		return Credential{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v2/plugin/auth/token/refresh", nil)
	if err != nil {
		return Credential{}, err
	}
	headers := authHeaders(base, realm, current.AccessToken)
	headers.Set("X-Refresh-Token", current.RefreshToken)
	headers.Set("X-Auth-Refresh-Source", "plugin")
	headers.Set("X-CodeBuddy-Request", "1")
	applyHeaders(request.Header, headers)
	response, err := client.Do(request)
	if err != nil {
		return Credential{}, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Credential{}, &TokenEndpointError{
			Status: response.StatusCode, Code: codeOf(body), Message: messageOf(body), Retry: retryAfterOf(response),
		}
	}
	envelope := decodeAuthEnvelope(body)
	if !envelope.ok() {
		return Credential{}, &TokenEndpointError{
			Status: response.StatusCode, Code: envelope.codeText(), Message: envelope.Msg,
		}
	}
	var token tokenPayload
	if err := json.Unmarshal(envelope.Data, &token); err != nil {
		return Credential{}, fmt.Errorf("parse WorkBuddy refresh response: %w", err)
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		return Credential{}, ErrCredentialIncomplete
	}
	if uid := strings.TrimSpace(token.UID); uid != "" && uid != current.UID {
		return Credential{}, ErrCredentialIdentityChanged
	}
	refreshed := current
	refreshed.AccessToken = strings.TrimSpace(token.AccessToken)
	if value := strings.TrimSpace(token.RefreshToken); value != "" {
		refreshed.RefreshToken = value
	}
	if domain := strings.TrimSpace(token.Domain); domain != "" {
		refreshed.Domain = domain
	}
	if expiresAt := expiryFromSeconds(token.ExpiresIn, time.Now()); expiresAt > 0 {
		refreshed.ExpiresAt = expiresAt
	}
	refreshed.Realm = realm
	refreshed.BaseURL = base
	return refreshed, nil
}

// RefreshLeadTime is how early a credential should be refreshed.
func RefreshLeadTime() time.Duration { return refreshLeadTime }

type authResponse struct {
	status   int
	body     []byte
	header   http.Header
	envelope authEnvelope
}

func (response *authResponse) envelopeCode() string    { return response.envelope.codeText() }
func (response *authResponse) envelopeMessage() string { return response.envelope.Msg }

func (response *authResponse) Close() {}

type authEnvelope struct {
	Code json.RawMessage `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func decodeAuthEnvelope(body []byte) authEnvelope {
	var envelope authEnvelope
	_ = json.Unmarshal(body, &envelope)
	if len(envelope.Data) == 0 {
		// 兼容 data 缺失时把根对象当作数据。
		envelope.Data = body
	}
	return envelope
}

func (envelope authEnvelope) ok() bool {
	code := envelope.codeText()
	return code == "" || code == "0"
}

func (envelope authEnvelope) codeText() string {
	return codeText(envelope.Code)
}

func codeText(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	if trimmed[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			return strings.TrimSpace(value)
		}
		return ""
	}
	return trimmed
}

func postJSON(ctx context.Context, client *http.Client, endpoint string, headers http.Header, body []byte) (*authResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	applyHeaders(request.Header, headers)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	return &authResponse{status: response.StatusCode, body: payload, header: response.Header.Clone(), envelope: decodeAuthEnvelope(payload)}, nil
}

func authHeaders(base, realm, accessToken string) http.Header {
	headers := make(http.Header)
	headers.Set("Accept", "application/json")
	headers.Set("Content-Type", "application/json")
	headers.Set("User-Agent", cliUserAgent)
	headers.Set("Accept-Language", languageFor(realm))
	headers.Set("X-Requested-With", "XMLHttpRequest")
	headers.Set("X-CodeBuddy-Request", "1")
	headers.Set("Origin", originFor(realm))
	headers.Set("Referer", originFor(realm)+"/")
	if accessToken != "" {
		headers.Set("Authorization", "Bearer "+accessToken)
	}
	return headers
}

func languageFor(realm string) string {
	if realm == realmCN {
		return "zh-CN"
	}
	return "en-US"
}

func applyHeaders(target, source http.Header) {
	for key, values := range source {
		target[key] = append([]string(nil), values...)
	}
}

func codeOf(body []byte) string {
	envelope := decodeAuthEnvelope(body)
	if code := envelope.codeText(); code != "" && code != "0" {
		return code
	}
	var nested struct {
		Error struct {
			Code json.RawMessage `json:"code"`
			Data struct {
				Code json.RawMessage `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &nested); err == nil {
		if code := codeText(nested.Error.Data.Code); code != "" {
			return code
		}
		if code := codeText(nested.Error.Code); code != "" {
			return code
		}
	}
	return ""
}

func messageOf(body []byte) string {
	envelope := decodeAuthEnvelope(body)
	if strings.TrimSpace(envelope.Msg) != "" {
		return envelope.Msg
	}
	var nested struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &nested); err == nil {
		return nested.Error.Message
	}
	return ""
}

func retryAfterOf(response *http.Response) time.Duration {
	raw := strings.TrimSpace(response.Header.Get("Retry-After"))
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(raw); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return 0
}

// AccountIdentity is used by the model catalog and execution paths.
func AccountIdentity(value Credential) string { return value.UID }
