package workbuddy

import (
	"context"
	"errors"
	"net"
	"strings"

	"gpt-load/internal/channel/modules"
	"gpt-load/internal/channel/spec"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

type workbuddyDriver struct{}

// Implementations returns the WorkBuddy subscription driver and model catalog.
func Implementations() subscriptionruntime.Implementations {
	return subscriptionruntime.Implementations{
		Drivers:           []subscriptionruntime.Driver{&workbuddyDriver{}},
		ModelDiscoveries:  []subscriptionruntime.ModelDiscovery{workbuddyModelDiscovery{}},
		QuotaObservations: []subscriptionruntime.QuotaObservation{workbuddyQuotaObservation{}},
	}
}

func (*workbuddyDriver) ID() spec.SubscriptionDriverID { return modules.WorkBuddySubscriptionDriver }

func (*workbuddyDriver) Parse(raw []byte) (subscriptionruntime.Credential, error) {
	value, err := ParseCredentialJSON(raw)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	return newRuntimeCredential(value)
}

// ImportCredential accepts the desktop auth file shape. Encrypted
// ($wbEncrypted) files are rejected explicitly by ParseCredentialJSON.
func (*workbuddyDriver) ImportCredential(_ context.Context, raw []byte) (subscriptionruntime.Credential, error) {
	value, err := ParseCredentialJSON(raw)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	return newRuntimeCredential(value)
}

func (*workbuddyDriver) Refresh(ctx context.Context, current subscriptionruntime.Credential) (subscriptionruntime.Credential, error) {
	value, err := ParseCredentialJSON(current.Canonical())
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	if strings.TrimSpace(value.RefreshToken) == "" {
		return subscriptionruntime.Credential{}, ErrCredentialIncomplete
	}
	refreshed, err := RefreshCredentialOnce(ctx, value)
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	return newRuntimeCredential(refreshed)
}

func (*workbuddyDriver) MatchesRefreshIdentity(current, refreshed subscriptionruntime.Credential) bool {
	return current.Identity() != "" && current.Identity() == refreshed.Identity()
}

func (*workbuddyDriver) ClassifyRefreshFailure(err error) subscriptionruntime.RefreshFailureDecision {
	if err == nil {
		return subscriptionruntime.RefreshFailureDecision{Kind: subscriptionruntime.RefreshFailureOutcomeUnknown}
	}
	if errors.Is(err, ErrCredentialIdentityChanged) {
		return subscriptionruntime.RefreshFailureDecision{Kind: subscriptionruntime.RefreshFailureIdentityChanged}
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

func (*workbuddyDriver) BeginDeviceAuthorization(ctx context.Context) (subscriptionruntime.DeviceAuthorization, error) {
	return BeginCredentialAuthorization(ctx)
}

func (*workbuddyDriver) PollDeviceAuthorization(ctx context.Context, state []byte) (subscriptionruntime.DeviceAuthorizationPoll, error) {
	return PollCredentialAuthorization(ctx, state)
}
