package trae

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

var (
	// ErrCredentialIncomplete 表示缺少 access/refresh token。
	ErrCredentialIncomplete = errors.New("Trae credential is missing an access token or refresh token")
	// ErrCredentialIdentityChanged 表示刷新后返回了另一个账号。
	ErrCredentialIdentityChanged = errors.New("refreshed Trae credential identity changed")
)

// Credential 是 Trae 订阅渠道的规范化凭证。
type Credential struct {
	Type         string `json:"type,omitempty"`
	Realm        string `json:"realm"`
	BaseURL      string `json:"base_url,omitempty"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ExpiresAt    int64  `json:"expires_at,omitempty"`

	UID         string `json:"uid,omitempty"`
	UserName    string `json:"user_name,omitempty"`
	Email       string `json:"email,omitempty"`
	StoreRegion string `json:"store_region,omitempty"`

	// 设备指纹：官方客户端按账号派生，缺失时由网关稳定派生。
	DeviceID    string `json:"device_id,omitempty"`
	MachineID   string `json:"machine_id,omitempty"`
	DeviceType  string `json:"device_type,omitempty"`
	DeviceBrand string `json:"device_brand,omitempty"`
	DeviceCPU   string `json:"device_cpu,omitempty"`
	OSVersion   string `json:"os_version,omitempty"`

	// DevicePrivateKey/DevicePublicKey 仅国际版刷新需要（P-256 SPKI）。
	DevicePrivateKey string `json:"device_private_key,omitempty"`
	DevicePublicKey  string `json:"device_public_key,omitempty"`

	AppVersion  string `json:"app_version,omitempty"`
	VersionCode string `json:"version_code,omitempty"`
}

// DisplayLabel 返回界面展示用的账号标识。
func (value Credential) DisplayLabel() string {
	for _, candidate := range []string{value.UserName, value.Email} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	if uid := strings.TrimSpace(value.UID); uid != "" {
		if len(uid) > 8 {
			return "Trae " + uid[:8]
		}
		return "Trae " + uid
	}
	return "Trae"
}

// ParseCredentialJSON 接受规范化 JSON 与社区工具导出的 camelCase 形态。
func ParseCredentialJSON(raw []byte, profile Profile) (Credential, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return Credential{}, ErrCredentialIncomplete
	}
	var flat struct {
		Type         string          `json:"type"`
		Realm        string          `json:"realm"`
		BaseURL      string          `json:"base_url"`
		AccessToken  string          `json:"access_token"`
		RefreshToken string          `json:"refresh_token"`
		ExpiresAt    json.RawMessage `json:"expires_at"`
		UID          string          `json:"uid"`
		UserID       string          `json:"user_id"`
		UserName     string          `json:"user_name"`
		Email        string          `json:"email"`
		StoreRegion  string          `json:"store_region"`

		DeviceID         string `json:"device_id"`
		MachineID        string `json:"machine_id"`
		DeviceType       string `json:"device_type"`
		DeviceBrand      string `json:"device_brand"`
		DeviceCPU        string `json:"device_cpu"`
		OSVersion        string `json:"os_version"`
		DevicePrivateKey string `json:"device_private_key"`
		DevicePublicKey  string `json:"device_public_key"`

		AppVersion  string `json:"app_version"`
		VersionCode string `json:"version_code"`

		// 社区工具常见 camelCase
		Token          string          `json:"token"`
		AccessTokenAlt string          `json:"accessToken"`
		RefreshAlt     string          `json:"refreshToken"`
		UserAlt        string          `json:"userId"`
		ExpiredAt      string          `json:"expiredAt"`
		ExpiresAtAlt   json.RawMessage `json:"expiresAt"`
	}
	if err := json.Unmarshal(trimmed, &flat); err != nil {
		return Credential{}, fmt.Errorf("parse Trae credential: %w", err)
	}
	accessToken := firstNonEmpty(flat.AccessToken, flat.AccessTokenAlt, flat.Token)
	refreshToken := firstNonEmpty(flat.RefreshToken, flat.RefreshAlt)
	credential := Credential{
		Type:             firstNonEmpty(flat.Type, "trae"),
		Realm:            normalizeRealm(flat.Realm, flat.BaseURL, profile),
		BaseURL:          strings.TrimSpace(flat.BaseURL),
		AccessToken:      accessToken,
		RefreshToken:     refreshToken,
		ExpiresAt:        firstNonZero(expiryFromRaw(flat.ExpiresAt), expiryFromRaw(flat.ExpiresAtAlt), expiryFromText(flat.ExpiredAt)),
		UID:              firstNonEmpty(flat.UID, flat.UserID, flat.UserAlt),
		UserName:         strings.TrimSpace(flat.UserName),
		Email:            strings.TrimSpace(flat.Email),
		StoreRegion:      strings.TrimSpace(flat.StoreRegion),
		DeviceID:         strings.TrimSpace(flat.DeviceID),
		MachineID:        strings.TrimSpace(flat.MachineID),
		DeviceType:       strings.TrimSpace(flat.DeviceType),
		DeviceBrand:      strings.TrimSpace(flat.DeviceBrand),
		DeviceCPU:        strings.TrimSpace(flat.DeviceCPU),
		OSVersion:        strings.TrimSpace(flat.OSVersion),
		DevicePrivateKey: strings.TrimSpace(flat.DevicePrivateKey),
		DevicePublicKey:  strings.TrimSpace(flat.DevicePublicKey),
		AppVersion:       firstNonEmpty(flat.AppVersion, profile.AppVersion),
		VersionCode:      firstNonEmpty(flat.VersionCode, profile.VersionCode),
	}
	if credential.AccessToken == "" && credential.RefreshToken == "" {
		return Credential{}, ErrCredentialIncomplete
	}
	credential.AccessToken = strings.TrimSpace(credential.AccessToken)
	credential.RefreshToken = strings.TrimSpace(credential.RefreshToken)
	if credential.UID == "" && credential.RefreshToken != "" {
		// 没有 uid 时用刷新令牌派生稳定身份，避免空 identity 拒绝刷新结果。
		credential.UID = "rt-" + shortHash(credential.RefreshToken)
	}
	return credential, nil
}

// Marshal 返回写回加密存储的规范化 JSON。
func (value Credential) Marshal() ([]byte, error) { return json.Marshal(value) }

// SecretValues 返回需要脱敏的值。
func (value Credential) SecretValues() []string {
	result := make([]string, 0, 4)
	for _, secret := range []string{value.AccessToken, value.RefreshToken, value.DevicePrivateKey, value.DevicePublicKey} {
		if trimmed := strings.TrimSpace(secret); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// ExpiresAtTime 返回 access token 过期时间。
func (value Credential) ExpiresAtTime() (time.Time, bool) {
	if value.ExpiresAt <= 0 {
		return time.Time{}, false
	}
	return time.Unix(value.ExpiresAt, 0), true
}

// EffectiveBaseURL 返回实际使用的核心网关地址。
func (value Credential) EffectiveBaseURL(profile Profile) string {
	raw := strings.TrimSpace(value.BaseURL)
	if raw == "" {
		return profile.CoreURL
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	return strings.TrimRight(raw, "/")
}

// NormalizeHosts 返回按优先级排序的候选主机（主 + 备选）。
func (value Credential) NormalizeHosts(base string, fallbacks []string) []string {
	hosts := make([]string, 0, len(fallbacks)+1)
	if trimmed := strings.TrimSpace(base); trimmed != "" {
		hosts = append(hosts, trimBaseURL(trimmed))
	}
	for _, fallback := range fallbacks {
		if trimmed := strings.TrimSpace(fallback); trimmed != "" {
			hosts = append(hosts, trimBaseURL(trimmed))
		}
	}
	return dedupeStrings(hosts)
}

func newRuntimeCredential(value Credential) (subscriptionruntime.Credential, error) {
	canonical, err := value.Marshal()
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	expiresAt, known := value.ExpiresAtTime()
	account := subscriptionruntime.Account{Email: value.DisplayLabel(), ExpiresAt: expiresAt, ExpiresAtKnown: known}
	return subscriptionruntime.NewCredential(canonical, value.UID, account, expiresAt, known, value.SecretValues()), nil
}

// normalizeRealm 按显式值或主机名判定区域。
func normalizeRealm(explicit, baseURL string, profile Profile) string {
	switch strings.ToLower(strings.TrimSpace(explicit)) {
	case realmCN:
		return realmCN
	case realmIntl:
		return realmIntl
	}
	host := hostOf(baseURL)
	switch {
	case host == "":
		if profile.Key != "" {
			return profile.Key
		}
		return realmIntl
	case strings.Contains(host, "trae-api-cn"), strings.Contains(host, "mchost.guru") && !strings.Contains(host, "-sg"),
		strings.HasSuffix(host, ".trae.cn"), strings.HasSuffix(host, "trae.com.cn"):
		return realmCN
	default:
		return realmIntl
	}
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

func trimBaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	return strings.TrimRight(raw, "/")
}

func dedupeStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func firstNonZero(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func expiryFromRaw(raw json.RawMessage) int64 {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return 0
	}
	if value, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return normalizeExpiry(value)
	}
	return expiryFromText(strings.Trim(trimmed, `"`))
}

func expiryFromText(text string) int64 {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0
	}
	if value, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return normalizeExpiry(value)
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed.Unix()
		}
	}
	return 0
}

func normalizeExpiry(value int64) int64 {
	if value <= 0 {
		return 0
	}
	// 上游使用毫秒时间戳（> 1e11）。
	if value > 100_000_000_000 {
		return value / 1000
	}
	return value
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}
