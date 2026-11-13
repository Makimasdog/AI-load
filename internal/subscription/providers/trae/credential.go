package trae

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
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
	DeviceName  string `json:"device_name,omitempty"`
	DeviceModel string `json:"device_model,omitempty"`
	OSInfo      string `json:"os_info,omitempty"`
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
	if uid := value.EffectiveUID(); uid != "" {
		if len(uid) > 8 {
			return "Trae " + uid[:8]
		}
		return "Trae " + uid
	}
	return "Trae"
}

// ParseCredentialJSON 接受规范化 JSON、社区工具导出的 camelCase 形态，
// 以及客户端 storage.json 的明文片段（国际版）。
func ParseCredentialJSON(raw []byte, profile Profile) (Credential, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return Credential{}, ErrCredentialIncomplete
	}
	if bytes.Contains(trimmed, []byte("iCubeAuthInfo://icube.cloudide")) {
		return parseStorageJSON(trimmed, profile)
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
		DeviceName       string `json:"device_name"`
		DeviceModel      string `json:"device_model"`
		OSInfo           string `json:"os_info"`
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
		DeviceName:       strings.TrimSpace(flat.DeviceName),
		DeviceModel:      strings.TrimSpace(flat.DeviceModel),
		OSInfo:           strings.TrimSpace(flat.OSInfo),
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

// EffectiveUID 返回可用于请求头的真实用户 id：凭据里的 UID 为空或仍是
// rt- 占位值时，从 access_token 的 JWT data.id 推导。占位值直接发给上游
// 会被判为非法参数或导致账号身份绑定失败。
func (value Credential) EffectiveUID() string {
	uid := strings.TrimSpace(value.UID)
	if uid != "" && !strings.HasPrefix(uid, "rt-") {
		return uid
	}
	if derived := uidFromToken(value.AccessToken); derived != "" {
		return derived
	}
	return uid
}

// uidFromToken 从 Trae 的 JWT 载荷里取 data.id（真实用户 id）。
func uidFromToken(token string) string {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(parts[1]))
	if err != nil {
		return ""
	}
	var claims struct {
		Data struct {
			ID json.Number `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return strings.TrimSpace(claims.Data.ID.String())
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
	// 运行时身份用 JWT 里的真实 id；canonical 原样保留，避免指纹变化。
	identity := firstNonEmpty(value.EffectiveUID(), value.UID)
	return subscriptionruntime.NewCredential(canonical, identity, account, expiresAt, known, value.SecretValues()), nil
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

// parseStorageJSON 解析客户端 storage.json 的明文片段（国际版）。
//
// 国内版的同名键是 tc 密文信封（base64），这里不做解密，直接给出明确错误，
// 避免用户以为"导入成功但调不通"。
func parseStorageJSON(raw []byte, profile Profile) (Credential, error) {
	var store map[string]json.RawMessage
	if err := json.Unmarshal(raw, &store); err != nil {
		return Credential{}, fmt.Errorf("解析 Trae storage.json 失败: %w", err)
	}
	authRaw, ok := store["iCubeAuthInfo://icube.cloudide"]
	if !ok {
		return Credential{}, fmt.Errorf("storage.json 中未找到 iCubeAuthInfo://icube.cloudide")
	}
	var authText string
	if err := json.Unmarshal(authRaw, &authText); err != nil {
		return Credential{}, fmt.Errorf("iCubeAuthInfo://icube.cloudide 不是字符串: %w", err)
	}
	trimmedAuth := strings.TrimSpace(authText)
	if !strings.HasPrefix(trimmedAuth, "{") {
		return Credential{}, ErrEncryptedStorage
	}
	var auth map[string]any
	decoder := json.NewDecoder(strings.NewReader(trimmedAuth))
	decoder.UseNumber()
	if err := decoder.Decode(&auth); err != nil {
		return Credential{}, fmt.Errorf("解析 iCubeAuthInfo 明文失败: %w", err)
	}
	credential := Credential{
		Type:         "trae",
		Realm:        profile.Key,
		AccessToken:  firstNonEmpty(lookupText(auth, "token", "accessToken", "access_token", "Token"), lookupText(auth, "jwt")),
		RefreshToken: lookupText(auth, "refreshToken", "refresh_token", "RefreshToken"),
		UID:          lookupText(auth, "userId", "user_id", "uid", "UserId"),
		UserName:     lookupText(auth, "userName", "username", "name"),
		Email:        lookupText(auth, "email", "Email"),
		StoreRegion:  lookupText(auth, "region", "storeRegion", "store_region"),
		ExpiresAt:    firstNonZero(expiryFromRaw(rawOf(auth, "expiredAt")), expiryFromRaw(rawOf(auth, "expiresAt")), expiryFromRaw(rawOf(auth, "expired_at"))),
		AppVersion:   firstNonEmpty(lookupText(auth, "appVersion"), profile.AppVersion),
		VersionCode:  firstNonEmpty(lookupText(auth, "versionCode"), profile.VersionCode),
	}
	// 设备密钥对：iCubeAuthInfo://icube-dc:<设备ID>
	for key, value := range store {
		if !strings.HasPrefix(key, "iCubeAuthInfo://icube-dc:") {
			continue
		}
		var keyText string
		if err := json.Unmarshal(value, &keyText); err != nil {
			continue
		}
		trimmedKey := strings.TrimSpace(keyText)
		if !strings.HasPrefix(trimmedKey, "{") {
			return Credential{}, ErrEncryptedStorage
		}
		var pair map[string]any
		if err := json.Unmarshal([]byte(trimmedKey), &pair); err != nil {
			continue
		}
		credential.DevicePrivateKey = lookupText(pair, "privateKeyPEM", "private_key_pem", "privateKey")
		credential.DevicePublicKey = lookupText(pair, "publicKeyPEM", "public_key_pem", "publicKey")
		if suffix := strings.TrimPrefix(key, "iCubeAuthInfo://icube-dc:"); credential.DeviceID == "" {
			credential.DeviceID = strings.TrimSpace(suffix)
		}
	}
	if rawMachine, ok := store["telemetry.machineId"]; ok {
		var machine string
		if err := json.Unmarshal(rawMachine, &machine); err == nil {
			credential.MachineID = strings.TrimSpace(strings.Trim(machine, `"`))
		}
	}
	credential.AccessToken = strings.TrimSpace(credential.AccessToken)
	credential.RefreshToken = strings.TrimSpace(credential.RefreshToken)
	if credential.AccessToken == "" && credential.RefreshToken == "" {
		return Credential{}, ErrCredentialIncomplete
	}
	if credential.UID == "" && credential.RefreshToken != "" {
		credential.UID = "rt-" + shortHash(credential.RefreshToken)
	}
	return credential, nil
}

func lookupText(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			if text := strings.TrimSpace(stringValue(value)); text != "" {
				return text
			}
		}
	}
	return ""
}

func rawOf(values map[string]any, key string) json.RawMessage {
	value, ok := values[key]
	if !ok {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return encoded
}
