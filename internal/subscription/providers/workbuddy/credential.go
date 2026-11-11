// Package workbuddy implements GPT-Load's WorkBuddy subscription contract.
//
// WorkBuddy has no published OpenAI-compatible API. This package speaks to the
// official desktop client's private gateway (OAuth login, token refresh and
// /v2/chat/completions). It is an unofficial integration: upstream may add or
// change channel verification at any time and break this channel.
package workbuddy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	realmGlobal = "global"
	realmCN     = "cn"

	defaultGlobalBaseURL = "https://www.workbuddy.ai"
	defaultCNBaseURL     = "https://copilot.tencent.com"

	globalDomain = "www.workbuddy.ai"
	cnOrigin     = "https://www.codebuddy.cn"
	globalOrigin = "https://www.workbuddy.ai"
)

var (
	ErrCredentialIdentityChanged = errors.New("refreshed WorkBuddy credential identity changed")
	ErrCredentialEncrypted       = errors.New("WorkBuddy credential is encrypted with $wbEncrypted and cannot be imported")
	ErrCredentialIncomplete      = errors.New("WorkBuddy credential is missing an access token or account uid")
)

// Credential is the canonical WorkBuddy subscription credential.
type Credential struct {
	Type         string `json:"type,omitempty"`
	Realm        string `json:"realm"`
	BaseURL      string `json:"base_url,omitempty"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ExpiresAt    int64  `json:"expires_at,omitempty"`
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterprise_id,omitempty"`
	Nickname     string `json:"nickname,omitempty"`
	Email        string `json:"email,omitempty"`
	Phone        string `json:"phone,omitempty"`
	Domain       string `json:"domain,omitempty"`
	DeviceToken  string `json:"device_token,omitempty"`
}

// DisplayLabel returns the readable account identifier shown in the UI.
// Nickname is what the login page renders, so it wins; the e-mail and phone
// number are the fallbacks the upstream account object may carry.
func (value Credential) DisplayLabel() string {
	for _, candidate := range []string{value.Nickname, value.Email, value.Phone} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	if uid := strings.TrimSpace(value.UID); uid != "" {
		if len(uid) > 8 {
			uid = uid[:8]
		}
		return "WorkBuddy " + uid
	}
	return "WorkBuddy"
}

// ParseCredentialJSON accepts the canonical form, the desktop auth file shape
// ({"auth":...,"account":...}) and the raw token-polling envelope
// ({"code":0,"data":{...}}).
func ParseCredentialJSON(raw []byte) (Credential, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return Credential{}, ErrCredentialIncomplete
	}
	probe := trimmed
	if len(probe) > 64 {
		probe = probe[:64]
	}
	if bytes.HasPrefix(trimmed, []byte("$wbEncrypted")) || bytes.Contains(probe, []byte("$wbEncrypted")) {
		return Credential{}, ErrCredentialEncrypted
	}

	var flat struct {
		Type         string          `json:"type"`
		Realm        string          `json:"realm"`
		BaseURL      string          `json:"base_url"`
		AccessToken  string          `json:"access_token"`
		RefreshToken string          `json:"refresh_token"`
		ExpiresAt    json.RawMessage `json:"expires_at"`
		UID          string          `json:"uid"`
		EnterpriseID string          `json:"enterprise_id"`
		Nickname     string          `json:"nickname"`
		Email        string          `json:"email"`
		Phone        string          `json:"phone"`
		Domain       string          `json:"domain"`
		DeviceToken  string          `json:"device_token"`
	}
	if err := json.Unmarshal(trimmed, &flat); err != nil {
		return Credential{}, fmt.Errorf("parse WorkBuddy credential: %w", err)
	}
	if strings.TrimSpace(flat.AccessToken) != "" {
		credential := Credential{
			Type: flat.Type, Realm: flat.Realm, BaseURL: flat.BaseURL,
			AccessToken: strings.TrimSpace(flat.AccessToken), RefreshToken: strings.TrimSpace(flat.RefreshToken),
			ExpiresAt: expiryFromRaw(flat.ExpiresAt), UID: strings.TrimSpace(flat.UID),
			EnterpriseID: strings.TrimSpace(flat.EnterpriseID), Nickname: strings.TrimSpace(flat.Nickname),
			Email: strings.TrimSpace(flat.Email), Phone: strings.TrimSpace(flat.Phone),
			Domain: strings.TrimSpace(flat.Domain), DeviceToken: strings.TrimSpace(flat.DeviceToken),
		}
		return normalizeCredential(credential)
	}

	var nested struct {
		Auth    *desktopAuth    `json:"auth"`
		Account *desktopAccount `json:"account"`
		Data    *tokenPayload   `json:"data"`
	}
	if err := json.Unmarshal(trimmed, &nested); err != nil {
		return Credential{}, fmt.Errorf("parse WorkBuddy credential: %w", err)
	}
	credential := Credential{Type: "workbuddy"}
	switch {
	case nested.Auth != nil:
		credential.AccessToken = strings.TrimSpace(nested.Auth.AccessToken)
		credential.RefreshToken = strings.TrimSpace(nested.Auth.RefreshToken)
		credential.ExpiresAt = expiryFromRaw(nested.Auth.ExpiresAt)
		credential.Domain = strings.TrimSpace(nested.Auth.Domain)
		credential.BaseURL = strings.TrimSpace(nested.Auth.BaseURL)
		if nested.Account != nil {
			credential.UID = strings.TrimSpace(nested.Account.UID)
			credential.EnterpriseID = strings.TrimSpace(nested.Account.EnterpriseID)
			credential.Nickname = nested.Account.nickname()
			credential.Email = strings.TrimSpace(nested.Account.Email)
			credential.Phone = nested.Account.phone()
		}
	case nested.Data != nil:
		credential.AccessToken = strings.TrimSpace(nested.Data.AccessToken)
		credential.RefreshToken = strings.TrimSpace(nested.Data.RefreshToken)
		credential.ExpiresAt = expiryFromSeconds(nested.Data.ExpiresIn, time.Now())
		credential.Domain = strings.TrimSpace(nested.Data.Domain)
		credential.UID = strings.TrimSpace(nested.Data.UID)
	default:
		return Credential{}, ErrCredentialIncomplete
	}
	return normalizeCredential(credential)
}

type desktopAuth struct {
	AccessToken  string          `json:"accessToken"`
	RefreshToken string          `json:"refreshToken"`
	ExpiresAt    json.RawMessage `json:"expiresAt"`
	Domain       string          `json:"domain"`
	BaseURL      string          `json:"baseUrl"`
}

type desktopAccount struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
	Name         string `json:"name"`
	Email        string `json:"email"`
	PhoneNumber  string `json:"phoneNumber"`
	Phone        string `json:"phone"`
	Mobile       string `json:"mobile"`
}

func (account desktopAccount) phone() string {
	for _, candidate := range []string{account.PhoneNumber, account.Phone, account.Mobile} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (account desktopAccount) nickname() string {
	for _, candidate := range []string{account.Nickname, account.Name} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

type tokenPayload struct {
	AccessToken  string      `json:"accessToken"`
	RefreshToken string      `json:"refreshToken"`
	ExpiresIn    json.Number `json:"expiresIn"`
	Domain       string      `json:"domain"`
	UID          string      `json:"uid"`
}

func normalizeCredential(value Credential) (Credential, error) {
	value.AccessToken = strings.TrimSpace(value.AccessToken)
	value.UID = strings.TrimSpace(value.UID)
	if value.AccessToken == "" || value.UID == "" {
		return Credential{}, ErrCredentialIncomplete
	}
	value.Realm = realmFor(value.Realm, value.BaseURL, value.Domain)
	value.BaseURL = normalizeBaseURL(value.BaseURL, value.Realm)
	if value.Domain == "" && value.Realm == realmGlobal {
		value.Domain = globalDomain
	}
	if value.Type == "" {
		value.Type = "workbuddy"
	}
	return value, nil
}

// Marshal returns the canonical JSON written back to encrypted storage.
func (value Credential) Marshal() ([]byte, error) {
	return json.Marshal(value)
}

func (value Credential) SecretValues() []string {
	result := make([]string, 0, 3)
	for _, secret := range []string{value.AccessToken, value.RefreshToken, value.DeviceToken} {
		if trimmed := strings.TrimSpace(secret); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func (value Credential) ExpiresAtTime() (time.Time, bool) {
	if value.ExpiresAt <= 0 {
		return time.Time{}, false
	}
	return time.Unix(value.ExpiresAt, 0), true
}

func (value Credential) EffectiveBaseURL() string {
	return normalizeBaseURL(value.BaseURL, value.Realm)
}

// RealmForBaseURL reports which gateway family a target belongs to.
func RealmForBaseURL(baseURL string) string {
	return realmFor("", baseURL, "")
}

func realmFor(explicit, baseURL, domain string) string {
	switch strings.ToLower(strings.TrimSpace(explicit)) {
	case realmCN:
		return realmCN
	case realmGlobal:
		return realmGlobal
	}
	for _, candidate := range []string{baseURL, domain} {
		host := hostOf(candidate)
		switch {
		case host == "":
			continue
		case host == "copilot.tencent.com", strings.HasSuffix(host, ".codebuddy.cn"),
			strings.HasSuffix(host, ".workbuddy.cn"), strings.HasSuffix(host, ".tencent.com"):
			return realmCN
		default:
			return realmGlobal
		}
	}
	return realmGlobal
}

func hostOf(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func normalizeBaseURL(raw, realm string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if realm == realmCN {
			return defaultCNBaseURL
		}
		return defaultGlobalBaseURL
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	return strings.TrimRight(raw, "/")
}

func expiryFromRaw(raw json.RawMessage) int64 {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return 0
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		// 允许 RFC3339 字符串。
		if parsed, parseErr := time.Parse(time.RFC3339, strings.Trim(trimmed, `"`)); parseErr == nil {
			return parsed.Unix()
		}
		return 0
	}
	return normalizeExpiry(value)
}

func normalizeExpiry(value int64) int64 {
	if value <= 0 {
		return 0
	}
	// 毫秒时间戳（> 1e11）归一为秒。
	if value > 100_000_000_000 {
		return value / 1000
	}
	return value
}

func expiryFromSeconds(value json.Number, now time.Time) int64 {
	seconds, err := value.Int64()
	if err != nil || seconds <= 0 {
		return 0
	}
	// 异常大（> 10 年）视为脏值，交由上层忽略。
	if seconds > int64(10*365*24*time.Hour/time.Second) {
		return 0
	}
	return now.Add(time.Duration(seconds) * time.Second).Unix()
}
