package trae

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

// 网页登录走客户端同款 loopback 回调：两个区域各自占用固定端口。
func (d *driver) BeginAuthorization() (subscriptionruntime.Authorization, error) {
	value, err := BeginBrowserAuthorization(d.profile)
	if err != nil {
		return subscriptionruntime.Authorization{}, err
	}
	driverState, err := json.Marshal(value.DeviceState)
	if err != nil {
		return subscriptionruntime.Authorization{}, err
	}
	return subscriptionruntime.Authorization{
		URL: value.AuthorizationURL, State: value.DeviceState.State,
		DriverState: driverState, ExpiresAt: value.ExpiresAt,
	}, nil
}

func (d *driver) LocalCallback() (subscriptionruntime.LocalCallbackSpec, bool) {
	return subscriptionruntime.LocalCallbackSpec{RedirectURI: RedirectURI(d.profile)}, true
}

func (d *driver) CompleteAuthorization(
	ctx context.Context,
	completion subscriptionruntime.AuthorizationCompletion,
) (subscriptionruntime.Credential, error) {
	var state AuthorizationDriverState
	if err := json.Unmarshal(completion.DriverState, &state); err != nil ||
		strings.TrimSpace(state.Verifier) == "" || strings.TrimSpace(state.PrivateKey) == "" {
		return subscriptionruntime.Credential{}, ErrAuthorizationStateInvalid
	}
	credential, err := CompleteBrowserAuthorization(ctx, d.profile, state, BrowserAuthorizationCompletion{
		ExpectedState: completion.ExpectedState, ReturnedState: completion.ReturnedState,
		Code: completion.Code,
	})
	if err != nil {
		return subscriptionruntime.Credential{}, err
	}
	return newRuntimeCredential(credential)
}

func (*driver) ParseAuthorizationCallback(query url.Values) (subscriptionruntime.AuthorizationCallback, error) {
	return ParseAuthorizationCallback(query)
}

func (*driver) AuthorizationStateRequired() bool { return AuthorizationStateRequired() }

func (*driver) AuthorizationFailureDefinitive(err error) bool {
	// state 不匹配时授权码还没有被交换，可以直接判定失败。
	if errors.Is(err, ErrAuthorizationStateInvalid) || errors.Is(err, ErrAuthorizationCodeMissing) {
		return true
	}
	var tokenError *TokenEndpointError
	if !errors.As(err, &tokenError) {
		return false
	}
	switch strings.TrimSpace(tokenError.Code) {
	case "20324", "20101", "20315", "20125", "20126", "20401", "20403", "20405":
		return true
	}
	switch tokenError.Status {
	case 400, 401, 403:
		return true
	default:
		return false
	}
}

var _ subscriptionruntime.BrowserAuthorizationDriver = (*driver)(nil)
var _ subscriptionruntime.CallbackQueryParser = (*driver)(nil)
