package modules

import (
	"gpt-load/internal/channel/spec"
	"gpt-load/internal/execution"
	"gpt-load/internal/protocol"
)

const (
	TraeSubscriptionDriver spec.SubscriptionDriverID = "trae"
	TraeModelDiscovery     spec.UtilityID            = "trae_models"
	TraeQuotaObservation   spec.UtilityID            = "trae_quota"

	TraeIntlSubscriptionDriver spec.SubscriptionDriverID = "trae_intl"
	TraeIntlModelDiscovery     spec.UtilityID            = "trae_intl_models"
	TraeIntlQuotaObservation   spec.UtilityID            = "trae_intl_quota"
)

// Trae declares the unofficial Trae (CN) subscription channel. The upstream
// surface is the official desktop client's private gateway, not a published
// OpenAI-compatible API.
func Trae() spec.Module {
	return spec.Module{Definition: spec.Definition{
		ID:          spec.Trae,
		Name:        "Trae",
		Mark:        "TR",
		Icon:        "trae",
		SearchTerms: []string{"trae", "bytedance", "trae cn", "ide", "subscription"},
		Description: "Unofficial Trae CN subscription gateway (may stop working without notice)",
		Connection: spec.Connection{
			Type:            spec.ConnectionSubscription,
			CredentialInput: "authorization",
			AuthorizationMethods: []spec.AuthorizationMethod{
				spec.AuthorizationBrowserOAuth,
				spec.AuthorizationOAuthFile,
			},
		},
		Params: []spec.Field{{
			Key: "base_url", Label: "API root URL", InputKind: spec.InputURL,
			Normalizer: spec.NormalizeOptionalHTTPSBaseURL,
		}},
		Credentials: []spec.Field{},
		Provider: spec.ProviderBinding{
			ProviderKind:    spec.ProviderTrae,
			EndpointPolicy:  spec.EndpointSDKDefault,
			DefaultBaseURLs: []string{"https://trae-api-cn.mchost.guru"},
		},
		Routes: []spec.Route{
			spec.NewRoute(protocol.OpenAICompletions, execution.OperationChatCompletion, execution.RouteNative),
			spec.NewRoute(protocol.OpenAICompletions, execution.OperationProbe, execution.RouteNative),
		},
		Capabilities: spec.CapabilityBindings{
			SubscriptionDriver: TraeSubscriptionDriver,
			ModelDiscovery:     TraeModelDiscovery,
			QuotaObservation:   TraeQuotaObservation,
		},
	}}
}

// TraeIntl declares the international Trae channel. It shares the provider
// implementation with Trae but keeps a separate realm, host set and credential
// domain.
func TraeIntl() spec.Module {
	return spec.Module{Definition: spec.Definition{
		ID:          spec.TraeIntl,
		Name:        "Trae (Intl)",
		Mark:        "TI",
		Icon:        "trae_intl",
		SearchTerms: []string{"trae", "trae ai", "international", "bytedance", "ide", "subscription"},
		Description: "Unofficial Trae international subscription gateway (may stop working without notice)",
		Connection: spec.Connection{
			Type:            spec.ConnectionSubscription,
			CredentialInput: "authorization",
			AuthorizationMethods: []spec.AuthorizationMethod{
				spec.AuthorizationBrowserOAuth,
				spec.AuthorizationOAuthFile,
			},
		},
		Params: []spec.Field{{
			Key: "base_url", Label: "API root URL", InputKind: spec.InputURL,
			Normalizer: spec.NormalizeOptionalHTTPSBaseURL,
		}},
		Credentials: []spec.Field{},
		Provider: spec.ProviderBinding{
			ProviderKind:    spec.ProviderTraeIntl,
			EndpointPolicy:  spec.EndpointSDKDefault,
			DefaultBaseURLs: []string{"https://core-normal.traeapi.us"},
		},
		Routes: []spec.Route{
			spec.NewRoute(protocol.OpenAICompletions, execution.OperationChatCompletion, execution.RouteNative),
			spec.NewRoute(protocol.OpenAICompletions, execution.OperationProbe, execution.RouteNative),
		},
		Capabilities: spec.CapabilityBindings{
			SubscriptionDriver: TraeIntlSubscriptionDriver,
			ModelDiscovery:     TraeIntlModelDiscovery,
			QuotaObservation:   TraeIntlQuotaObservation,
		},
	}}
}
