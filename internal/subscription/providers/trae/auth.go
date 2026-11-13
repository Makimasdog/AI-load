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
	"net/http"
	"strconv"
	"strings"
	"time"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	// exchangeTokenPathV3 是当前客户端（Trae CN 3.4.18 / 国际版 3.5.x）使用的续期路径。
	exchangeTokenPathV3 = "/trae/api/v3/oauth/ExchangeToken"
	// exchangeTokenPathLegacy 是旧客户端的续期路径，仅作回退。
	exchangeTokenPathLegacy = "/cloudide/api/v3/trae/oauth/ExchangeToken"
	userInfoPath            = "/cloudide/api/v3/trae/GetUserInfo"

	maxAuthResponseBytes = 1 << 20
)

// 凭证缺失时的明确错误：避免被当成网络故障或上游拒绝。
var (
	ErrMissingAccessToken  = errors.New("该 Trae 凭证缺少 access_token，请重新导入或先执行刷新")
	ErrMissingRefreshToken = errors.New("该 Trae 凭证缺少 refresh_token，无法刷新；请重新导入")
	ErrIntlMissingDeviceKey = errors.New("Trae 国际版刷新需要设备私钥（iCubeAuthInfo://icube-dc:* 中的 privateKeyPEM），当前凭证未提供")
	ErrEncryptedStorage     = errors.New("该 storage.json 里的登录态是 tc 密文封装，无法直接解析；请用社区工具导出明文 JSON 后再导入")
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
// 设备指纹/证明类错误（20403/20405）也归入此类：必须换一份完整凭证。
func (err *TokenEndpointError) NeedsReauthorization() bool {
	if err == nil {
		return false
	}
	if err.Status == http.StatusUnauthorized || err.Status == http.StatusForbidden {
		return true
	}
	switch strings.TrimSpace(err.Code) {
	case "20401", "20403", "20405":
		return true
	}
	return false
}

type exchangeEndpoint struct{ host, path string }

// RefreshCredentialOnce 用刷新令牌换取新的访问令牌。
//
// 上游是轮换式刷新：每次都会下发新的 refresh token，调用方必须先落盘再使用。
// 续期和签名细节完全对齐客户端 out/main.js 的 exchangeTokenByRefreshToken()：
//
//	canonical = "POST\n<path>\n<ClientID>\n<RefreshToken>\n<unix秒>\n<32位hex nonce>"
//	Signature = base64(ECDSA-P256-SHA256(canonical))  // DER 编码
func RefreshCredentialOnce(ctx context.Context, profile Profile, current Credential) (Credential, error) {
	if strings.TrimSpace(current.RefreshToken) == "" {
		return Credential{}, ErrMissingRefreshToken
	}
	if profile.Key == realmIntl && strings.TrimSpace(current.DevicePrivateKey) == "" {
		return Credential{}, ErrIntlMissingDeviceKey
	}
	var lastErr error
	for _, endpoint := range refreshEndpoints(profile, current) {
		payload := map[string]any{
			"ClientID":     clientIDIDE,
			"ClientSecret": "",
			"RefreshToken": current.RefreshToken,
			"UserID":       strings.TrimSpace(current.UID),
			"IDEVersion":   firstNonEmpty(current.AppVersion, profile.AppVersion),
		}
		if strings.TrimSpace(current.DevicePrivateKey) != "" {
			info, proof, err := buildDeviceProof(profile, endpoint.path, current)
			if err != nil {
				return Credential{}, err
			}
			payload["DeviceInfo"] = info
			payload["DeviceProof"] = proof
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return Credential{}, err
		}
		response, err := postJSON(ctx, profile, endpoint.host+endpoint.path, body, "")
		if err != nil {
			lastErr = err
			var tokenError *TokenEndpointError
			if errors.As(err, &tokenError) && (tokenError.Status == http.StatusNotFound || tokenError.Status == http.StatusMethodNotAllowed) {
				// 主机/路径迁移，换下一个候选。
				continue
			}
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
		return refreshed, nil
	}
	if lastErr != nil {
		return Credential{}, lastErr
	}
	return Credential{}, errors.New("Trae 没有可用的续期端点")
}

// refreshEndpoints 返回按优先级排序的续期候选（主机 × 路径）。
// 无法从客户端确认国际版的权威主机，因此按社区实测分布依次尝试。
func refreshEndpoints(profile Profile, value Credential) []exchangeEndpoint {
	hosts := []string{trimBaseURL(value.AccountHost(profile))}
	for _, fallback := range profile.AccountFallbacks {
		hosts = append(hosts, trimBaseURL(fallback))
	}
	hosts = dedupeStrings(hosts)
	result := make([]exchangeEndpoint, 0, len(hosts)*2)
	for _, host := range hosts {
		result = append(result, exchangeEndpoint{host: host, path: exchangeTokenPathV3})
	}
	// 旧路径只在最近一次候选上尝试，避免对每台主机都发一次无效请求。
	if len(hosts) > 0 {
		result = append(result, exchangeEndpoint{host: hosts[0], path: exchangeTokenPathLegacy})
	}
	return result
}

// FetchUserIdentity 在导入后补全账号身份（失败不阻断导入）。
func FetchUserIdentity(ctx context.Context, profile Profile, value Credential) (Credential, error) {
	if strings.TrimSpace(value.AccessToken) == "" {
		return Credential{}, ErrMissingAccessToken
	}
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
	// 客户端真实请求只带 Content-Type 与 x-cloudide-token；续期时该 token 为空串。
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
// DeviceProof
// ---------------------------------------------------------------------------

// buildDeviceProof 复刻客户端的 pFe()：
//
//	timestamp = floor(now/1000)                       // 秒
//	nonce     = 16 随机字节的 hex（32 字符）
//	canonical = [method, path, clientID, refreshToken, timestamp, nonce] join "\n"
//	signature = base64(ECDSA-P256-SHA256(canonical))  // ASN.1 DER
//
// 与 XCool-603/trae2api 的 Go 实现逐项一致；hub/magpie 的「空格拼接 + 毫秒」
// 属于另一套早期实现，已被客户端源码证伪，这里不再采用。
func buildDeviceProof(profile Profile, path string, value Credential) (map[string]any, map[string]any, error) {
	privateKey, err := parseECPrivateKey(value.DevicePrivateKey)
	if err != nil {
		return nil, nil, err
	}
	timestamp := time.Now().Unix()
	nonce := randomHex(16)
	canonical := strings.Join([]string{
		http.MethodPost, path, clientIDIDE, value.RefreshToken, strconv.FormatInt(timestamp, 10), nonce,
	}, "\n")
	digest := sha256.Sum256([]byte(canonical))
	signature, err := ecdsa.SignASN1(rand.Reader, privateKey, digest[:])
	if err != nil {
		return nil, nil, err
	}
	info := map[string]any{
		"DeviceID":        firstNonEmpty(value.DeviceID, deriveDeviceID(value.UID)),
		"MachineID":       firstNonEmpty(value.MachineID, deriveMachineID(value.UID)),
		"PlatformCode":    "IDE_PC",
		"DeviceType":      firstNonEmpty(value.DeviceType, "PC"),
		"DeviceName":      firstNonEmpty(value.DeviceName, "DESKTOP"),
		"DeviceModel":     firstNonEmpty(value.DeviceModel, ""),
		"ClientVersion":   firstNonEmpty(value.AppVersion, profile.AppVersion),
		"DevicePublicKey": strings.TrimSpace(value.DevicePublicKey),
		"DeviceBrand":     firstNonEmpty(value.DeviceBrand, ""),
		"DeviceCPU":       firstNonEmpty(value.DeviceCPU, ""),
		"OSInfo":          firstNonEmpty(value.OSInfo, "Windows"),
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
		return nil, errors.New("Trae 设备私钥为空")
	}
	block, _ := pem.Decode([]byte(trimmed))
	if block == nil {
		return nil, errors.New("Trae 设备私钥不是 PEM 编码")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if ecdsaKey, ok := key.(*ecdsa.PrivateKey); ok {
			return ecdsaKey, nil
		}
		return nil, fmt.Errorf("Trae 设备私钥不是 EC 密钥（实际为 %T）", key)
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, errors.New("Trae 设备私钥无法解析（PKCS#8 / SEC1 均失败）")
}

func randomHex(size int) string {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		sum := sha256.Sum256([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		return hex.EncodeToString(sum[:size])
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
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
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
		Message string `json:"message"`
		Msg     string `json:"msg"`
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
