package trae

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	exchangeTokenPath = "/cloudide/api/v3/trae/oauth/ExchangeToken"
	userInfoPath      = "/cloudide/api/v3/trae/GetUserInfo"

	maxAuthResponseBytes = 1 << 20
)

// TokenEndpointError 描述刷新/身份接口的拒绝结果。
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
		return fmt.Sprintf("Trae endpoint rejected the request (status=%d code=%s)", err.Status, code)
	case code != "":
		return fmt.Sprintf("Trae endpoint rejected the request (code=%s)", code)
	case err.Status != 0:
		return fmt.Sprintf("Trae endpoint rejected the request (status=%d)", err.Status)
	default:
		return "Trae endpoint rejected the request"
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

// NeedsReauthorization 表示刷新令牌已失效，只能重新登录。
func (err *TokenEndpointError) NeedsReauthorization() bool {
	if err == nil {
		return false
	}
	return err.Status == http.StatusUnauthorized || err.Status == http.StatusForbidden
}

// RefreshCredentialOnce 用刷新令牌换取新的访问令牌。
// 上游是轮换式刷新：每次都会下发新的 refresh token，调用方必须先落盘再使用。
func RefreshCredentialOnce(ctx context.Context, profile Profile, current Credential) (Credential, error) {
	if strings.TrimSpace(current.RefreshToken) == "" {
		return Credential{}, ErrCredentialIncomplete
	}
	payload := map[string]any{
		"ClientID":     clientIDIDE,
		"RefreshToken": current.RefreshToken,
		"ClientSecret": clientSecret,
		"UserID":       strings.TrimSpace(current.UID),
		"IDEVersion":   firstNonEmpty(current.AppVersion, profile.AppVersion),
	}
	if profile.Key == realmIntl {
		if info, proof, err := buildDeviceProof(profile, exchangeTokenPath, current); err == nil {
			payload["DeviceInfo"] = info
			payload["DeviceProof"] = proof
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Credential{}, err
	}
	response, err := postJSON(ctx, profile, current.AccountHost(profile)+exchangeTokenPath, body, "")
	if err != nil {
		return Credential{}, err
	}
	result := decodeResult(response.body)
	token := firstNonEmpty(stringValue(result["Token"]), stringValue(result["token"]))
	if token == "" {
		return Credential{}, &TokenEndpointError{
			Status: response.status, Code: firstNonEmpty(codeOf(response.body), "refresh_failed"),
			Message: firstNonEmpty(stringValue(result["Message"]), messageOf(response.body)),
		}
	}
	if uid := firstNonEmpty(stringValue(result["UserId"]), stringValue(result["UID"])); uid != "" &&
		current.UID != "" && !strings.HasPrefix(current.UID, "rt-") && uid != current.UID {
		return Credential{}, ErrCredentialIdentityChanged
	}
	refreshed := current
	refreshed.AccessToken = token
	if value := firstNonEmpty(stringValue(result["RefreshToken"]), stringValue(result["refreshToken"])); value != "" {
		refreshed.RefreshToken = value
	}
	if raw := result["TokenExpireAt"]; raw != nil {
		refreshed.ExpiresAt = normalizeExpiry(numeric(raw))
	}
	refreshed.Realm = profile.Key
	if value := firstNonEmpty(stringValue(result["UserJwt"])); value != "" {
		// UserJwt 是身份 JWT，不需要持久化；保留该分支便于排障时判断上游行为。
		_ = value
	}
	return refreshed, nil
}

// FetchUserIdentity 在导入后补全账号身份（失败不阻断导入）。
func FetchUserIdentity(ctx context.Context, profile Profile, value Credential) (Credential, error) {
	body, _ := json.Marshal(map[string]any{
		"ReqSource":  "IDE",
		"IDEVersion": firstNonEmpty(value.AppVersion, profile.AppVersion),
	})
	response, err := postJSON(ctx, profile, value.AccountHost(profile)+userInfoPath, body, value.AccessToken)
	if err != nil {
		return Credential{}, err
	}
	result := decodeResult(response.body)
	updated := value
	if uid := firstNonEmpty(stringValue(result["UserId"]), stringValue(result["UID"]), stringValue(result["uid"])); uid != "" {
		updated.UID = uid
	}
	if name := firstNonEmpty(stringValue(result["UserName"]), stringValue(result["UserNameCN"]), stringValue(result["Nickname"])); name != "" {
		updated.UserName = name
	}
	if email := firstNonEmpty(stringValue(result["Email"]), stringValue(result["UserEmail"])); email != "" {
		updated.Email = email
	}
	if region := firstNonEmpty(stringValue(result["StoreRegion"]), stringValue(result["UserRegion"])); region != "" {
		updated.StoreRegion = region
	}
	return updated, nil
}

// AccountHost 返回 OAuth/身份接口主机（与核心网关分离）。
func (value Credential) AccountHost(profile Profile) string {
	host := strings.TrimSpace(profile.AccountURL)
	if host == "" {
		return profile.CoreURL
	}
	return trimBaseURL(host)
}

type authResponse struct {
	status int
	body   []byte
	header http.Header
}

func postJSON(ctx context.Context, profile Profile, endpoint string, body []byte, accessToken string) (*authResponse, error) {
	client, err := subscriptionruntime.HTTPClient(ctx)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", profile.UserAgent)
	request.Header.Set("x-cloudide-token", strings.TrimSpace(accessToken))
	if accessToken != "" {
		request.Header.Set("Authorization", profile.AuthPrefix+" "+accessToken)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	result := &authResponse{status: response.StatusCode, body: payload, header: response.Header.Clone()}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return result, &TokenEndpointError{
			Status: response.StatusCode, Code: firstNonEmpty(codeOf(payload), "http_error"),
			Message: messageOf(payload), Retry: retryAfterOf(response),
		}
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// DeviceProof（仅国际版）
// ---------------------------------------------------------------------------

func buildDeviceProof(profile Profile, path string, value Credential) (map[string]any, map[string]any, error) {
	privateKey, err := parseECPrivateKey(value.DevicePrivateKey)
	if err != nil {
		return nil, nil, err
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce := randomHex(16)
	parts := []string{http.MethodPost, path, clientIDIDE, value.RefreshToken, timestamp, nonce}
	separator := " "
	if !profile.ProofSpaced {
		separator = "\n"
	}
	raw := strings.Join(parts, separator)
	digest := sha256.Sum256([]byte(raw))
	r, s, err := ecdsa.Sign(rand.Reader, privateKey, digest[:])
	if err != nil {
		return nil, nil, err
	}
	signature := append(paddedBytes(r), paddedBytes(s)...)
	info := map[string]any{
		"DeviceID":        firstNonEmpty(value.DeviceID, deriveDeviceID(value.UID)),
		"MachineID":       firstNonEmpty(value.MachineID, deriveMachineID(value.UID)),
		"PlatformCode":    "Win",
		"DeviceType":      firstNonEmpty(value.DeviceType, "PC"),
		"DeviceName":      "DESKTOP",
		"DeviceModel":     "PC",
		"ClientVersion":   firstNonEmpty(value.AppVersion, profile.AppVersion),
		"DevicePublicKey": strings.TrimSpace(value.DevicePublicKey),
		"DeviceBrand":     firstNonEmpty(value.DeviceBrand, "Generic"),
		"DeviceCPU":       firstNonEmpty(value.DeviceCPU, "x64"),
		"OSInfo":          "Windows",
		"OSVersion":       firstNonEmpty(value.OSVersion, "10.0"),
	}
	proof := map[string]any{
		"Signature": base64.StdEncoding.EncodeToString(signature),
		"Timestamp": timestamp,
		"Nonce":     nonce,
	}
	return info, proof, nil
}

func parseECPrivateKey(raw string) (*ecdsa.PrivateKey, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("Trae device private key is empty")
	}
	block, _ := pem.Decode([]byte(trimmed))
	if block == nil {
		return nil, errors.New("Trae device private key is not PEM encoded")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if ecdsaKey, ok := key.(*ecdsa.PrivateKey); ok {
			return ecdsaKey, nil
		}
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, errors.New("Trae device private key is not a P-256 ECDSA key")
}

func paddedBytes(value *big.Int) []byte {
	result := make([]byte, 32)
	raw := value.Bytes()
	copy(result[32-len(raw):], raw)
	return result
}

func randomHex(size int) string {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return hex.EncodeToString([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))[:size*2]
	}
	return hex.EncodeToString(buffer)
}

// ---------------------------------------------------------------------------
// 响应解析
// ---------------------------------------------------------------------------

func decodeResult(body []byte) map[string]any {
	var envelope struct {
		Result json.RawMessage `json:"Result"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		for _, raw := range []json.RawMessage{envelope.Result, envelope.Data} {
			if len(raw) == 0 {
				continue
			}
			var result map[string]any
			if err := json.Unmarshal(raw, &result); err == nil && len(result) > 0 {
				return result
			}
		}
	}
	var flat map[string]any
	if err := json.Unmarshal(body, &flat); err == nil {
		return flat
	}
	return map[string]any{}
}

func codeOf(body []byte) string {
	var envelope struct {
		Code             json.RawMessage `json:"code"`
		Message          string          `json:"message"`
		ResponseMetadata struct {
			Error struct {
				Code    json.RawMessage `json:"Code"`
				Message string          `json:"Message"`
			} `json:"Error"`
		} `json:"ResponseMetadata"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	if value := codeText(envelope.Code); value != "" && value != "0" {
		return value
	}
	return codeText(envelope.ResponseMetadata.Error.Code)
}

func messageOf(body []byte) string {
	var envelope struct {
		Message          string `json:"message"`
		Msg              string `json:"msg"`
		ResponseMetadata struct {
			Error struct {
				Message string `json:"Message"`
			} `json:"Error"`
		} `json:"ResponseMetadata"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	return firstNonEmpty(envelope.Message, envelope.Msg, envelope.ResponseMetadata.Error.Message)
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

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func numeric(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed
	default:
		return 0
	}
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
