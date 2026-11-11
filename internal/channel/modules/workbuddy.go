package modules

import (
	"gpt-load/internal/channel/spec"
	"gpt-load/internal/execution"
	"gpt-load/internal/protocol"
)

const (
	WorkBuddySubscriptionDriver spec.SubscriptionDriverID = "workbuddy"
	WorkBuddyModelDiscovery     spec.UtilityID            = "workbuddy_models"
	WorkBuddyQuotaObservation   spec.UtilityID            = "workbuddy_quota"
)

// WorkBuddy declares the unofficial WorkBuddy subscription channel. The
// upstream surface is the official desktop client's private gateway, not a
// published OpenAI-compatible API.
func WorkBuddy() spec.Module {
	return spec.Module{Definition: spec.Definition{
		ID:          spec.WorkBuddy,
		Name:        "WorkBuddy",
		Mark:        "WB",
		Icon:        "workbuddy",
		SearchTerms: []string{"workbuddy", "tencent", "codebuddy", "subscription", "oauth"},
		Description: "Unofficial WorkBuddy subscription gateway (may stop working without notice)",
		Connection: spec.Connection{
			Type:            spec.ConnectionSubscription,
			CredentialInput: "authorization",
			AuthorizationMethods: []spec.AuthorizationMethod{
				spec.AuthorizationDeviceOAuth,
				spec.AuthorizationOAuthFile,
			},
		},
		Params: []spec.Field{{
			Key: "base_url", Label: "API root URL", InputKind: spec.InputURL,
			Normalizer: spec.NormalizeOptionalHTTPSBaseURL,
		}},
		Credentials: []spec.Field{},
		Provider: spec.ProviderBinding{
			ProviderKind:    spec.ProviderWorkBuddy,
			EndpointPolicy:  spec.EndpointSDKDefault,
			DefaultBaseURLs: []string{"https://www.workbuddy.ai"},
		},
		Routes: []spec.Route{
			spec.NewRoute(protocol.OpenAICompletions, execution.OperationChatCompletion, execution.RouteNative),
			// The credential probe is a real (minimal) chat completion, so it
			// exercises the upstream instead of reporting "unsupported".
			spec.NewRoute(protocol.OpenAICompletions, execution.OperationProbe, execution.RouteNative),
		},
		Capabilities: spec.CapabilityBindings{
			SubscriptionDriver: WorkBuddySubscriptionDriver,
			ModelDiscovery:     WorkBuddyModelDiscovery,
			QuotaObservation:   WorkBuddyQuotaObservation,
		},
	}}
}
