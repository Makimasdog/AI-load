package trae

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestBeginBrowserAuthorizationBuildsClientURL(t *testing.T) {
	t.Parallel()
	profile := CNProfile
	value, err := BeginBrowserAuthorization(profile)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(value.AuthorizationURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "https" || parsed.Host != "www.trae.cn" || parsed.Path != "/authorization" {
		t.Fatalf("authorization URL = %q", value.AuthorizationURL)
	}
	query := parsed.Query()
	if query.Get("client_id") != clientIDIDE || query.Get("auth_from") != "trae" ||
		query.Get("login_channel") != authorizationLoginChannel ||
		query.Get("code_challenge_method") != "S256" ||
		query.Get("login_trace_id") != value.DeviceState.State ||
		query.Get("code_challenge") == "" {
		t.Fatalf("authorization query = %#v", query)
	}
	callback := query.Get("auth_callback_url")
	if !strings.HasPrefix(callback, RedirectURI(profile)+"?state=") {
		t.Fatalf("auth_callback_url = %q", callback)
	}
	if value.DeviceState.DeviceID == "" || len(value.DeviceState.DeviceID) != 19 ||
		len(value.DeviceState.MachineID) != 64 ||
		!strings.Contains(value.DeviceState.PrivateKey, "PRIVATE KEY") ||
		!strings.Contains(value.DeviceState.PublicKey, "PUBLIC KEY") {
		t.Fatalf("device state = %#v", value.DeviceState)
	}
	digest := sha256.Sum256([]byte(value.DeviceState.Verifier))
	if query.Get("code_challenge") != base64.RawURLEncoding.EncodeToString(digest[:]) {
		t.Fatal("code_challenge does not derive from the stored verifier")
	}
}

func TestBeginBrowserAuthorizationRequiresConsoleHost(t *testing.T) {
	t.Parallel()
	if _, err := BeginBrowserAuthorization(Profile{Key: realmCN}); err == nil {
		t.Fatal("expected missing console host to be rejected")
	}
}

func TestParseAuthorizationCallbackReadsAuthCodeInfo(t *testing.T) {
	t.Parallel()
	info, err := json.Marshal(map[string]string{"AuthCode": "code-123"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseAuthorizationCallback(url.Values{
		"state":        {"abc"},
		"authCodeInfo": {string(info)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.State != "abc" || parsed.Code != "code-123" || parsed.ProviderError != "" {
		t.Fatalf("parsed = %#v", parsed)
	}
}

func TestParseAuthorizationCallbackFallsBackToTraceID(t *testing.T) {
	t.Parallel()
	info := `{"AuthCode":"code-456"}`
	parsed, err := ParseAuthorizationCallback(url.Values{
		"login_trace_id": {"trace-1"},
		"authCodeInfo":   {info},
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.State != "trace-1" || parsed.Code != "code-456" {
		t.Fatalf("parsed = %#v", parsed)
	}
}

func TestParseAuthorizationCallbackRejectsEmptyCode(t *testing.T) {
	t.Parallel()
	if _, err := ParseAuthorizationCallback(url.Values{"state": {"abc"}}); err == nil {
		t.Fatal("expected missing code to be rejected")
	}
	if _, err := ParseAuthorizationCallback(url.Values{
		"state": {"abc"}, "authCodeInfo": {"not-json"},
	}); err == nil {
		t.Fatal("expected malformed authCodeInfo to be rejected")
	}
}

func TestParseAuthorizationCallbackReportsProviderError(t *testing.T) {
	t.Parallel()
	parsed, err := ParseAuthorizationCallback(url.Values{
		"state": {"abc"}, "error_code": {"access_denied"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ProviderError != "access_denied" || parsed.Code != "" {
		t.Fatalf("parsed = %#v", parsed)
	}
}

func TestCompleteBrowserAuthorizationExchangesWithoutDeviceProof(t *testing.T) {
	t.Parallel()
	var exchangeBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case exchangeTokenPathV3:
			if token := request.Header.Get("x-cloudide-token"); token != "" {
				t.Errorf("exchange x-cloudide-token = %q, want empty", token)
			}
			if authorization := request.Header.Get("Authorization"); authorization != "" {
				t.Errorf("exchange Authorization = %q, want empty", authorization)
			}
			if err := json.NewDecoder(request.Body).Decode(&exchangeBody); err != nil {
				t.Error(err)
			}
			_, _ = writer.Write([]byte(`{"Result":{"Token":"access-token","RefreshToken":"refresh-token","UserId":"user-1","TokenExpireAt":1900000000000}}`))
		case userInfoPath:
			_, _ = writer.Write([]byte(`{"Result":{"UserId":"user-1","Email":"owner@example.com"}}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	profile := CNProfile
	profile.AccountURL = server.URL
	profile.AccountFallbacks = nil
	authorization, err := BeginBrowserAuthorization(profile)
	if err != nil {
		t.Fatal(err)
	}
	credential, err := CompleteBrowserAuthorization(context.Background(), profile, authorization.DeviceState,
		BrowserAuthorizationCompletion{
			ExpectedState: authorization.DeviceState.State,
			ReturnedState: authorization.DeviceState.State,
			Code:          "authorization-code",
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := exchangeBody["DeviceProof"]; present {
		t.Fatal("authorization exchange must not send DeviceProof")
	}
	deviceInfo, _ := exchangeBody["DeviceInfo"].(map[string]any)
	if deviceInfo["DevicePublicKey"] != strings.TrimSpace(authorization.DeviceState.PublicKey) {
		t.Fatalf("DeviceInfo = %#v", deviceInfo)
	}
	if exchangeBody["AuthCode"] != "authorization-code" ||
		exchangeBody["CodeVerifier"] != authorization.DeviceState.Verifier {
		t.Fatalf("exchange body = %#v", exchangeBody)
	}
	if credential.AccessToken != "access-token" || credential.RefreshToken != "refresh-token" ||
		credential.UID != "user-1" || credential.Email != "owner@example.com" ||
		credential.DevicePrivateKey != strings.TrimSpace(authorization.DeviceState.PrivateKey) ||
		credential.DeviceID != authorization.DeviceState.DeviceID ||
		credential.Realm != realmCN {
		t.Fatalf("credential = %#v", credential)
	}
	if credential.ExpiresAt != 1900000000 {
		t.Fatalf("expires_at = %d", credential.ExpiresAt)
	}
}

func TestCompleteBrowserAuthorizationRejectsStateMismatch(t *testing.T) {
	t.Parallel()
	authorization, err := BeginBrowserAuthorization(CNProfile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = CompleteBrowserAuthorization(context.Background(), CNProfile, authorization.DeviceState,
		BrowserAuthorizationCompletion{
			ExpectedState: authorization.DeviceState.State,
			ReturnedState: "someone-else",
			Code:          "authorization-code",
		})
	if err != ErrAuthorizationStateInvalid {
		t.Fatalf("err = %v", err)
	}
}

func TestRedirectURIIsPerRealm(t *testing.T) {
	t.Parallel()
	if RedirectURI(CNProfile) == RedirectURI(IntlProfile) {
		t.Fatal("CN and INTL must not share a loopback callback")
	}
	if RedirectURI(CNProfile) != "http://127.0.0.1:54546/authorize" ||
		RedirectURI(IntlProfile) != "http://127.0.0.1:54547/authorize" {
		t.Fatalf("redirect URIs = %q, %q", RedirectURI(CNProfile), RedirectURI(IntlProfile))
	}
}
