// Package trae implements GPT-Load's Trae subscription contract.
//
// Trae is ByteDance's AI IDE. It has no published OpenAI-compatible API; this
// package speaks to the client's private gateway (refresh-token login and
// /api/ide/*/llm_raw_chat). It is an unofficial integration: upstream may add
// or change channel verification at any time and break these channels.
//
// 国内版与国际版是两个独立渠道，共用本包代码，差异全部收敛在 Profile。
package trae

import (
	"context"
	"errors"
	"net"
	"strings"

	"gpt-load/internal/channel/modules"
	"gpt-load/internal/channel/spec"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

const (
	realmCN   = "cn"
	realmIntl = "intl"

	// appID 是 Trae 桌面客户端的固定应用常量，非账号维度机密。
	appID = "6eefa01c-1036-4c7e-9ca5-d891f63bfcd8"
	// pluginChannel 标识请求来自 IDE 插件通道。
	pluginChannel = "icube-ai"

	// clientID 由「是不是 SOLO」决定：IDE 用 ono9krqynydwx5。
	clientIDIDE = "ono9krqynydwx5"
	// clientSecret 在 IDE 刷新流程里固定为 "-"。
	clientSecret = "-"
)

// Profile 固化两个区域网关的差异，避免散落的 if 判断。
type Profile struct {
	Key       string
	Label     string
	ChannelID spec.ID
	DriverID  spec.SubscriptionDriverID
	ModelsID  spec.UtilityID
	QuotaID   spec.UtilityID

	CoreURL       string
	CoreFallbacks []string
	// ConsoleURL 是网页登录授权页所在的站点（客户端 product.json 的
	// consoleHost，Trae 的 loginHost 与它相同）。
	ConsoleURL string
	AccountURL string
	// AccountFallbacks 是 OAuth/身份主机的备选，续期时按顺序尝试。
	AccountFallbacks []string
	PayURL           string
	PayFallback      string

	ChatPath      string
	ChatFallback  string
	ModelListPath string
	EntUsagePath  string
	PayStatusPath string

	ModelListSupported bool
	// AuthPrefix 是 Authorization 方案：CN 用 Cloud-IDE-JWT，INTL 用 Bearer。
	AuthPrefix string
	UserAgent  string
	// AppVersion/VersionCode 会被上游用于版本校验，可被凭证覆盖。
	AppVersion  string
	VersionCode string
	// BuildVersion 是客户端 product.json 的 tronBuildVersion，登录 URL 的
	// plugin_version 必须用它；为空时退回 "local"。
	BuildVersion string
	Region       string
	StoreCountry string
}

var (
	// CNProfile 是国内版渠道（trae）。
	CNProfile = Profile{
		Key: realmCN, Label: "Trae（国内版）",
		ChannelID: spec.Trae, DriverID: modules.TraeSubscriptionDriver,
		ModelsID: modules.TraeModelDiscovery, QuotaID: modules.TraeQuotaObservation,
		CoreURL:          "https://trae-api-cn.mchost.guru",
		ConsoleURL:       "https://www.trae.cn",
		AccountURL:       "https://api.trae.cn",
		AccountFallbacks: []string{"https://api.trae.com.cn"},
		PayURL:           "https://api.trae.cn", PayFallback: "https://api.trae.com.cn",
		ChatPath: "/api/ide/v2/llm_raw_chat", ChatFallback: "/api/ide/v1/llm_raw_chat",
		ModelListPath:      "/api/ide/v1/model_list",
		EntUsagePath:       "/trae/api/v2/pay/ide_user_ent_usage",
		PayStatusPath:      "/trae/api/v2/pay/ide_user_pay_status",
		ModelListSupported: true,
		AuthPrefix:         "Cloud-IDE-JWT",
		UserAgent:          "Trae CN/3.4.18",
		AppVersion:         "3.4.18", VersionCode: "20250325",
		BuildVersion: "2.3.102387",
		Region:       "CN",
	}
	// IntlProfile 是国际版渠道（trae-intl）。
	IntlProfile = Profile{
		Key: realmIntl, Label: "Trae（国际版）",
		ChannelID: spec.TraeIntl, DriverID: modules.TraeIntlSubscriptionDriver,
		ModelsID: modules.TraeIntlModelDiscovery, QuotaID: modules.TraeIntlQuotaObservation,
		CoreURL:       "https://core-normal.traeapi.us",
		CoreFallbacks: []string{"https://core-normal.trae.ai", "https://coresg-normal.trae.ai", "https://coreva-normal.trae.ai"},
		ConsoleURL:    "https://www.trae.ai",
		// 客户端的 apiHost 取自 product.json 的 bootConfig.account.trae.normal；
		// 国际版是 ug-normal.trae.ai（api.trae.ai 只有 nginx 404，grow-normal 是另一个服务）。
		AccountURL:       "https://ug-normal.trae.ai",
		AccountFallbacks: []string{"https://ug-normal.traeapi.us", "https://grow-normal.trae.ai"},
		PayURL:           "https://grow-normal.traeapi.us", PayFallback: "https://grow-normal.trae.ai",
		ChatPath:           "/api/ide/v1/llm_raw_chat",
		ModelListPath:      "/api/ide/v1/model_list",
		EntUsagePath:       "/trae/api/v1/pay/ide_user_ent_usage",
		PayStatusPath:      "/trae/api/v1/pay/ide_user_pay_status",
		ModelListSupported: false,
		AuthPrefix:         "Bearer",
		// 实测：INTL 用 "TraeClient/TTNet" 作 UA 时上游会按另一套模型
		// 注册表处理并返回 4023（model unknown）；改用客户端品牌 UA
		// 才能拿到真实业务码（如 4015 权限/区域限制）。
		UserAgent:  "Trae/3.6.2",
		AppVersion: "3.6.2", VersionCode: "20260924",
		Region: "US", StoreCountry: "us",
	}
)

// Implementations returns both Trae subscription drivers and their utilities.
func Implementations() subscriptionruntime.Implementations {
	return subscriptionruntime.Implementations{
		Drivers:           []subscriptionruntime.Driver{&driver{profile: CNProfile}, &driver{profile: IntlProfile}},
		ModelDiscoveries:  []subscriptionruntime.ModelDiscovery{modelDiscovery{profile: CNProfile}, modelDiscovery{profile: IntlProfile}},
		QuotaObservations: []subscriptionruntime.QuotaObservation{quotaObservation{profile: CNProfile}, quotaObservation{profile: IntlProfile}},
	}
}

type driver struct{ profile Profile }

func (d *driver) ID() spec.SubscriptionDriverID { return d.profile.DriverID }

func (d *driver) Parse(raw []byte) (subscriptionruntime.Credential, error) {
	value, err := ParseCredentialJSON(raw, d.profile)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	return newRuntimeCredential(value)
}

// ImportCredential accepts the canonical JSON form, or the flat/camelCase
// shapes exported by community tools.
func (d *driver) ImportCredential(_ context.Context, raw []byte) (subscriptionruntime.Credential, error) {
	value, err := ParseCredentialJSON(raw, d.profile)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	// 国际版只有 refresh token 而无设备私钥时，刷新必然返回 20405；
	// 在导入阶段直接拒绝，避免用户以为导入成功。
	if d.profile.Key == realmIntl && value.AccessToken == "" && strings.TrimSpace(value.DevicePrivateKey) == "" {
		return subscriptionruntime.Credential{}, ErrIntlMissingDeviceKey
	}
	return newRuntimeCredential(value)
}

func (d *driver) Refresh(ctx context.Context, current subscriptionruntime.Credential) (subscriptionruntime.Credential, error) {
	value, err := ParseCredentialJSON(current.Canonical(), d.profile)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	if strings.TrimSpace(value.AccessToken) == "" && strings.TrimSpace(value.RefreshToken) == "" {
		return subscriptionruntime.Credential{}, ErrCredentialIncomplete
	}
	refreshed, err := RefreshCredentialOnce(ctx, d.profile, value)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	return newRuntimeCredential(refreshed)
}

func (d *driver) MatchesRefreshIdentity(current, refreshed subscriptionruntime.Credential) bool {
	if current.Identity() == "" || refreshed.Identity() == "" {
		return false
	}
	return current.Identity() == refreshed.Identity()
}

func (d *driver) ClassifyRefreshFailure(err error) subscriptionruntime.RefreshFailureDecision {
	if err == nil {
		return subscriptionruntime.RefreshFailureDecision{Kind: subscriptionruntime.RefreshFailureOutcomeUnknown}
	}
	var tokenError *TokenEndpointError
	if errors.As(err, &tokenError) {
		decision := subscriptionruntime.RefreshFailureDecision{
			StatusCode: tokenError.Status, OAuthCode: strings.TrimSpace(tokenError.Code), RetryAfter: tokenError.Retry,
		}
		switch {
		case tokenError.NeedsReauthorization():
			decision.Kind = subscriptionruntime.RefreshFailureReauthorizationRequired
		case tokenError.Status == 0 || tokenError.Status == 429 || tokenError.Status >= 500:
			decision.Kind = subscriptionruntime.RefreshFailureRetryable
			if decision.RetryAfter <= 0 {
				decision.RetryAfter = subscriptionruntime.DefaultRefreshFailureCooldown
			}
		case subscriptionruntime.TokenEndpointFailureRetryable(tokenError.Status, tokenError.Code):
			decision.Kind = subscriptionruntime.RefreshFailureRetryable
		default:
			decision.Kind = subscriptionruntime.RefreshFailureOutcomeUnknown
		}
		return decision
	}
	var netError net.Error
	if errors.As(err, &netError) {
		return subscriptionruntime.RefreshFailureDecision{
			Kind: subscriptionruntime.RefreshFailureRetryable, RetryAfter: subscriptionruntime.DefaultRefreshFailureCooldown,
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return subscriptionruntime.RefreshFailureDecision{
			Kind: subscriptionruntime.RefreshFailureRetryable, RetryAfter: subscriptionruntime.DefaultRefreshFailureCooldown,
		}
	}
	return subscriptionruntime.RefreshFailureDecision{Kind: subscriptionruntime.RefreshFailureOutcomeUnknown}
}
