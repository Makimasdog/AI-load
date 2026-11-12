package trae

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	// 与客户端 oauthLocalServer.js 的 AUTHORIZE 常量一致。
	authorizationCallbackPath = "/authorize"
	// 两个区域是独立渠道，必须各自占用固定的 loopback 端口。
	callbackPortCN   = "54546"
	callbackPortIntl = "54547"

	authorizationTTL = 5 * time.Minute

	// 客户端固定写法：IDE 通道 + stable 应用档位。
	authorizationLoginChannel  = "native_ide"
	authorizationPluginVersion = "local"
	authorizationAppType       = "stable"
)

var (
	// ErrAuthorizationStateInvalid 表示回调 state 与发起登录时不一致。
	ErrAuthorizationStateInvalid = errors.New("Trae 授权状态校验失败，请重新发起登录")
	// ErrAuthorizationCodeMissing 表示回调没有携带可交换的授权码。
	ErrAuthorizationCodeMissing = errors.New("Trae 回调未携带授权码（authCodeInfo），请重新登录")
	// ErrAuthorizationUnsupported 表示当前渠道没有配置控制台登录地址。
	ErrAuthorizationUnsupported = errors.New("该 Trae 渠道未配置控制台登录地址，无法使用网页登录")
)

// BrowserAuthorization 是一次待完成的网页登录会话。
type BrowserAuthorization struct {
	AuthorizationURL string
	DeviceState      AuthorizationDriverState
	ExpiresAt        time.Time
}

// AuthorizationDriverState 是需要加密保存到阶段载荷里的会话机密。
// 设备密钥对在服务端生成：登录只登记公钥，私钥留给后续刷新签名。
type AuthorizationDriverState struct {
	State      string `json:"state,omitempty"`
	Verifier   string `json:"verifier"`
	PrivateKey string `json:"private_key"`
	PublicKey  string `json:"public_key"`
	DeviceID   string `json:"device_id"`
	MachineID  string `json:"machine_id"`
}

// BrowserAuthorizationCompletion 是 loopback 回调带回的一次性输入。
type BrowserAuthorizationCompletion struct {
	ExpectedState string
	ReturnedState string
	Code          string
}

// RedirectURI 返回该区域渠道固定的 loopback 回调地址。
func RedirectURI(profile Profile) string {
	port := callbackPortCN
	if profile.Key == realmIntl {
		port = callbackPortIntl
	}
	return "http://127.0.0.1:" + port + authorizationCallbackPath
}

// BeginBrowserAuthorization 构造与客户端完全一致的授权页 URL，并在服务端
// 生成 P-256 设备密钥与 PKCE verifier。
//
// 客户端登录时只把公钥放进 DeviceInfo（ExchangeToken 不要求 DeviceProof），
// 私钥留给后续刷新签名，因此服务端自生成设备密钥即可完成同一条链路。
func BeginBrowserAuthorization(profile Profile) (BrowserAuthorization, error) {
	consoleURL := strings.TrimRight(strings.TrimSpace(profile.ConsoleURL), "/")
	if consoleURL == "" {
		return BrowserAuthorization{}, ErrAuthorizationUnsupported
	}
	verifier, challenge, err := newPKCE()
	if err != nil {
		return BrowserAuthorization{}, err
	}
	privateKeyPEM, publicKeyPEM, err := newDeviceKeyPair()
	if err != nil {
		return BrowserAuthorization{}, err
	}
	state := newUUID()
	deviceID := randomDigits(19)
	machineID := randomHex(32)

	// 授权页会用 ^http://127.0.0.1:(\d+)/authorize$ 校验回调地址，
	// 因此这里不能附带任何 query（state 只能经 login_trace_id 关联）。
	redirect := RedirectURI(profile)
	pluginVersion := strings.TrimSpace(profile.BuildVersion)
	if pluginVersion == "" {
		pluginVersion = authorizationPluginVersion
	}
	// 与客户端 buildLoginUrl 完全一致：回调地址原样拼接，不额外编码。
	base := fmt.Sprintf(
		"%s/authorization?login_version=1&auth_from=trae&login_channel=%s&plugin_version=%s"+
			"&auth_type=local&client_id=%s&redirect=0&login_trace_id=%s"+
			"&auth_callback_url=%s&machine_id=%s&device_id=%s",
		consoleURL, authorizationLoginChannel, pluginVersion, clientIDIDE,
		state, redirect, machineID, deviceID,
	)
	fingerprint := url.Values{}
	fingerprint.Set("x_device_id", deviceID)
	fingerprint.Set("x_machine_id", machineID)
	fingerprint.Set("x_device_type", "Windows")
	fingerprint.Set("x_os_version", "10.0")
	fingerprint.Set("x_app_version", profile.AppVersion)
	fingerprint.Set("x_app_type", authorizationAppType)
	fingerprint.Set("code_challenge", challenge)
	fingerprint.Set("code_challenge_method", "S256")

	return BrowserAuthorization{
		AuthorizationURL: base + "&" + fingerprint.Encode(),
		DeviceState: AuthorizationDriverState{
			State: state, Verifier: verifier, PrivateKey: privateKeyPEM,
			PublicKey: publicKeyPEM, DeviceID: deviceID, MachineID: machineID,
		},
		ExpiresAt: time.Now().Add(authorizationTTL),
	}, nil
}

// CompleteBrowserAuthorization 用授权码换取 token，并把设备公钥登记到账号上。
// 该请求与客户端 exchangeTokenByAuthCode 一致：不带 DeviceProof。
func CompleteBrowserAuthorization(
	ctx context.Context,
	profile Profile,
	state AuthorizationDriverState,
	completion BrowserAuthorizationCompletion,
) (Credential, error) {
	returnedState := strings.TrimSpace(completion.ReturnedState)
	if returnedState != "" && strings.TrimSpace(completion.ExpectedState) != "" &&
		returnedState != strings.TrimSpace(completion.ExpectedState) {
		return Credential{}, ErrAuthorizationStateInvalid
	}
	code := strings.TrimSpace(completion.Code)
	if code == "" {
		return Credential{}, ErrAuthorizationCodeMissing
	}
	deviceInfo := map[string]any{
		"DeviceID":        firstNonEmpty(state.DeviceID, randomDigits(19)),
		"MachineID":       firstNonEmpty(state.MachineID, randomHex(32)),
		"PlatformCode":    "IDE_PC",
		"DeviceType":      "PC",
		"DeviceName":      deviceName(state.MachineID),
		"DeviceModel":     "",
		"ClientVersion":   profile.AppVersion,
		"DevicePublicKey": strings.TrimSpace(state.PublicKey),
		"DeviceBrand":     "",
		"DeviceCPU":       "",
		"OSInfo":          "Windows",
		"OSVersion":       "10.0",
	}
	payload := map[string]any{
		"ClientID":     clientIDIDE,
		"AuthCode":     code,
		"CodeVerifier": strings.TrimSpace(state.Verifier),
		"DeviceInfo":   deviceInfo,
		"IDEVersion":   profile.AppVersion,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Credential{}, err
	}
	for _, endpoint := range exchangeEndpoints(profile) {
		response, err := postJSON(ctx, profile, endpoint.host+endpoint.path, body, "")
		if err != nil {
			var tokenError *TokenEndpointError
			if errors.As(err, &tokenError) &&
				(tokenError.Status == http.StatusNotFound || tokenError.Status == http.StatusMethodNotAllowed) {
				continue
			}
			return Credential{}, err
		}
		result := decodeResult(response.body)
		token := firstNonEmpty(stringValue(result["Token"]), stringValue(result["token"]))
		if token == "" {
			return Credential{}, &TokenEndpointError{
				Status: response.status,
				Code:   firstNonEmpty(codeOf(response.body), "authorization_exchange_failed"),
				Message: firstNonEmpty(
					stringValue(result["Message"]), messageOf(response.body),
				),
			}
		}
		credential := Credential{
			Type:             "trae",
			Realm:            profile.Key,
			AccessToken:      token,
			RefreshToken:     firstNonEmpty(stringValue(result["RefreshToken"]), stringValue(result["refreshToken"])),
			UID:              firstNonEmpty(stringValue(result["UserId"]), stringValue(result["UID"])),
			DevicePrivateKey: strings.TrimSpace(state.PrivateKey),
			DevicePublicKey:  strings.TrimSpace(state.PublicKey),
			DeviceID:         firstNonEmpty(state.DeviceID, deviceInfo["DeviceID"].(string)),
			MachineID:        firstNonEmpty(state.MachineID, deviceInfo["MachineID"].(string)),
			AppVersion:       profile.AppVersion,
			VersionCode:      profile.VersionCode,
		}
		if raw := result["TokenExpireAt"]; raw != nil {
			credential.ExpiresAt = normalizeExpiry(numeric(raw))
		}
		if credential.UID == "" && credential.RefreshToken != "" {
			credential.UID = "rt-" + shortHash(credential.RefreshToken)
		}
		// 身份补全失败不阻断登录：token 已可用的凭证仍值得落库。
		if updated, enrichErr := FetchUserIdentity(ctx, profile, credential); enrichErr == nil {
			credential = updated
		}
		return credential, nil
	}
	return Credential{}, errors.New("Trae 没有可用的授权码换取端点")
}

// ParseAuthorizationCallback 读取 Trae 控制台回调。授权码位于 authCodeInfo
// （JSON 字符串）内，state 不保证回传，缺失时由 control 按渠道关联最近阶段。
func ParseAuthorizationCallback(query url.Values) (subscriptionruntime.AuthorizationCallback, error) {
	result := subscriptionruntime.AuthorizationCallback{
		// 授权页回调使用 loginTraceID；保留 state/login_trace_id 以兼容手工粘贴。
		State: firstNonEmpty(
			query.Get("loginTraceID"), query.Get("login_trace_id"), query.Get("state"),
		),
	}
	if raw := strings.TrimSpace(query.Get("authCodeInfo")); raw != "" {
		code, err := authCodeFromInfo(raw)
		if err != nil {
			return subscriptionruntime.AuthorizationCallback{}, err
		}
		result.Code = code
	} else {
		result.Code = strings.TrimSpace(query.Get("code"))
	}
	result.ProviderError = firstNonEmpty(query.Get("error_code"), query.Get("error"))
	if result.ProviderError != "" {
		result.Code = ""
		return result, nil
	}
	if result.Code == "" {
		return subscriptionruntime.AuthorizationCallback{}, ErrAuthorizationCodeMissing
	}
	return result, nil
}

// AuthorizationStateRequired 返回 false：Trae 的控制台回调不保证回传 state。
func AuthorizationStateRequired() bool { return false }

func authCodeFromInfo(raw string) (string, error) {
	decode := func(value string) (string, bool) {
		var info struct {
			AuthCode string `json:"AuthCode"`
		}
		if err := json.Unmarshal([]byte(value), &info); err != nil {
			return "", false
		}
		return strings.TrimSpace(info.AuthCode), true
	}
	if code, ok := decode(raw); ok && code != "" {
		return code, nil
	}
	if unescaped, err := url.QueryUnescape(raw); err == nil {
		if code, ok := decode(unescaped); ok && code != "" {
			return code, nil
		}
	}
	return "", errors.New("Trae 回调里的 authCodeInfo 无法解析")
}

// exchangeEndpoints 是授权码换取 token 的候选主机；路径与刷新一致。
func exchangeEndpoints(profile Profile) []exchangeEndpoint {
	hosts := []string{trimBaseURL(profile.AccountURL)}
	for _, fallback := range profile.AccountFallbacks {
		hosts = append(hosts, trimBaseURL(fallback))
	}
	hosts = dedupeStrings(hosts)
	result := make([]exchangeEndpoint, 0, len(hosts))
	for _, host := range hosts {
		if host == "" {
			continue
		}
		result = append(result, exchangeEndpoint{host: host, path: exchangeTokenPathV3})
	}
	return result
}

func newPKCE() (string, string, error) {
	buffer := make([]byte, 48)
	if _, err := rand.Read(buffer); err != nil {
		return "", "", err
	}
	verifier := base64.RawURLEncoding.EncodeToString(buffer)
	digest := sha256.Sum256([]byte(verifier))
	return verifier, base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func newDeviceKeyPair() (string, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", err
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", "", err
	}
	privatePEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}))
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	return privatePEM, publicPEM, nil
}

func randomDigits(size int) string {
	digits := make([]byte, size)
	random := make([]byte, size)
	if _, err := rand.Read(random); err != nil {
		sum := sha256.Sum256([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		copy(random, sum[:])
	}
	for index := range digits {
		digits[index] = '0' + random[index]%10
	}
	if digits[0] == '0' {
		digits[0] = '1'
	}
	return string(digits)
}

// newUUID 生成客户端同款的 v4 UUID（登录页把它作为 login_trace_id 回传）。
func newUUID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		sum := sha256.Sum256([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		copy(value, sum[:16])
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}

func deviceName(machineID string) string {
	return "DESKTOP-" + strings.ToUpper(shortHash(machineID)[:8])
}
