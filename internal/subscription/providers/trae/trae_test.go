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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestParseCredentialJSONAcceptsCommunityShape(t *testing.T) {
	raw := []byte(`{
		"token":"access-1",
		"refreshToken":"refresh-1",
		"userId":"1234567890123456789",
		"base_url":"trae-api-cn.mchost.guru",
		"expiresAt": 1765000000000
	}`)
	credential, err := ParseCredentialJSON(raw, CNProfile)
	if err != nil {
		t.Fatalf("ParseCredentialJSON: %v", err)
	}
	if credential.Realm != realmCN {
		t.Fatalf("realm = %q", credential.Realm)
	}
	if credential.AccessToken != "access-1" || credential.RefreshToken != "refresh-1" {
		t.Fatalf("tokens were not normalized: %+v", credential)
	}
	if credential.UID != "1234567890123456789" {
		t.Fatalf("uid = %q", credential.UID)
	}
	if credential.ExpiresAt != 1765000000 {
		t.Fatalf("expires at = %d", credential.ExpiresAt)
	}
	if got := credential.EffectiveBaseURL(CNProfile); got != "https://trae-api-cn.mchost.guru" {
		t.Fatalf("base url = %q", got)
	}
}

func TestParseCredentialJSONDerivesIdentityWithoutUID(t *testing.T) {
	credential, err := ParseCredentialJSON([]byte(`{"refresh_token":"abc"}`), IntlProfile)
	if err != nil {
		t.Fatalf("ParseCredentialJSON: %v", err)
	}
	if !strings.HasPrefix(credential.UID, "rt-") {
		t.Fatalf("derived uid = %q", credential.UID)
	}
	if credential.Realm != realmIntl {
		t.Fatalf("realm = %q", credential.Realm)
	}
}

func TestParseCredentialJSONRejectsEmptyTokens(t *testing.T) {
	if _, err := ParseCredentialJSON([]byte(`{"realm":"cn"}`), CNProfile); err == nil {
		t.Fatal("expected ErrCredentialIncomplete")
	}
}

func TestNormalizeRequestBodyProducesPartsArray(t *testing.T) {
	input := []byte(`{
		"model":"glm-5.2",
		"messages":[
			{"role":"developer","content":"be brief"},
			{"role":"user","content":[{"type":"text","text":"hi"}]}
		],
		"temperature":0.3,
		"seed":42,
		"max_completion_tokens":128
	}`)
	body, err := normalizeRequestBody(input, "glm-5.2", Credential{UID: "u1"}, CNProfile)
	if err != nil {
		t.Fatalf("normalizeRequestBody: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["model_name"] != "glm-5.2" {
		t.Fatalf("model_name = %v", payload["model_name"])
	}
	if _, ok := payload["seed"]; ok {
		t.Fatal("unsupported parameter seed was forwarded")
	}
	if payload["max_tokens"] != float64(128) {
		t.Fatalf("max_tokens = %v", payload["max_tokens"])
	}
	messages, ok := payload["messages"].([]any)
	if !ok || len(messages) != 2 {
		t.Fatalf("messages = %#v", payload["messages"])
	}
	first := messages[0].(map[string]any)
	if first["role"] != "system" {
		t.Fatalf("developer role was not mapped to system: %v", first["role"])
	}
	parts, ok := first["content"].([]any)
	if !ok || len(parts) != 1 {
		t.Fatalf("content = %#v", first["content"])
	}
	part := parts[0].(map[string]any)
	if part["type"] != "text" || part["text"] != "be brief" {
		t.Fatalf("part = %#v", part)
	}
}

func TestExecutorTranslatesSSEToOpenAIChunks(t *testing.T) {
	var gotPath string
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotPath = request.URL.Path
		gotAuth = request.Header.Get("Authorization")
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: metadata\ndata: {\"conversation_id\":\"c1\"}\n\n"))
		_, _ = writer.Write([]byte("event: output\ndata: {\"response\":\"Hel\"}\n\n"))
		_, _ = writer.Write([]byte("event: output\ndata: {\"reasoning_content\":\"let me think about it for a while\"}\n\n"))
		_, _ = writer.Write([]byte("event: output\ndata: {\"response\":\"lo\"}\n\n"))
		_, _ = writer.Write([]byte("event: token_usage\ndata: {\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}\n\n"))
		_, _ = writer.Write([]byte("event: done\ndata: {\"finish_reason\":\"stop\"}\n\n"))
	}))
	defer server.Close()

	executor := NewExecutor(CNProfile)
	response, err := executor.ExecuteStream(context.Background(), "cred", Credential{AccessToken: "tok", UID: "u1"}, ExecuteRequest{
		AttemptID: "a1", Model: "glm-5.2", Format: "openai", BaseURL: server.URL,
		Payload: []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	var contents []string
	var reasoning []string
	var finish string
	var usage map[string]any
	for chunk := range response.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		var payload struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage map[string]any `json:"usage"`
		}
		if err := json.Unmarshal(chunk.Payload, &payload); err != nil {
			t.Fatalf("decode chunk: %v", err)
		}
		if len(payload.Usage) > 0 {
			usage = payload.Usage
		}
		for _, choice := range payload.Choices {
			if choice.Delta.Content != "" {
				contents = append(contents, choice.Delta.Content)
			}
			if choice.Delta.ReasoningContent != "" {
				reasoning = append(reasoning, choice.Delta.ReasoningContent)
			}
			if choice.FinishReason != nil {
				finish = *choice.FinishReason
			}
		}
	}
	if strings.Join(contents, "") != "Hello" {
		t.Fatalf("content = %q", strings.Join(contents, ""))
	}
	if strings.Join(reasoning, "") != "let me think about it for a while" {
		t.Fatalf("reasoning = %q", strings.Join(reasoning, ""))
	}
	if len(reasoning) > 1 {
		t.Fatalf("reasoning was not coalesced: %d chunks", len(reasoning))
	}
	if finish != "stop" {
		t.Fatalf("finish reason = %q", finish)
	}
	if usage["total_tokens"] != float64(9) {
		t.Fatalf("usage = %#v", usage)
	}
	if gotPath != CNProfile.ChatPath {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Cloud-IDE-JWT tok" {
		t.Fatalf("authorization = %q", gotAuth)
	}
}

func TestExecutorAggregatesNonStreamCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: output\ndata: {\"response\":\"Hello\"}\n\n"))
		_, _ = writer.Write([]byte("event: output\ndata: {\"response\":\" world\"}\n\n"))
		_, _ = writer.Write([]byte("event: token_usage\ndata: {\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}\n\n"))
		_, _ = writer.Write([]byte("event: done\ndata: {\"finish_reason\":\"stop\"}\n\n"))
	}))
	defer server.Close()

	executor := NewExecutor(IntlProfile)
	response, err := executor.Execute(context.Background(), "cred", Credential{AccessToken: "tok", UID: "u1"}, ExecuteRequest{
		AttemptID: "a2", Model: "gpt-5.5", Format: "openai", BaseURL: server.URL,
		Payload: []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var payload struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(response.Payload, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.Object != "chat.completion" || len(payload.Choices) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Choices[0].Message.Content != "Hello world" || payload.Choices[0].FinishReason != "stop" {
		t.Fatalf("choice = %#v", payload.Choices[0])
	}
	if payload.Usage.TotalTokens != 5 {
		t.Fatalf("usage = %#v", payload.Usage)
	}
}

func TestClassifyHTTPErrorKeepsRiskControlAccountScoped(t *testing.T) {
	cases := []struct {
		code       string
		wantStatus int
		request    bool
		credential bool
		model      bool
	}{
		{code: "4019", wantStatus: http.StatusForbidden, credential: true},
		{code: "4015", wantStatus: http.StatusForbidden, request: true},
		{code: "4023", wantStatus: http.StatusBadRequest, model: true},
		{code: "4001", wantStatus: http.StatusBadRequest, request: true},
		{code: "1001", wantStatus: http.StatusUnauthorized, credential: true},
	}
	for _, test := range cases {
		err := classifyHTTPError(http.StatusOK, []byte(`{"code":"`+test.code+`","message":"x"}`))
		if err.Status != test.wantStatus {
			t.Fatalf("%s status = %d, want %d", test.code, err.Status, test.wantStatus)
		}
		if err.IsRequestScoped() != test.request || err.IsCredentialScoped() != test.credential || err.IsModelScoped() != test.model {
			t.Fatalf("%s scope = request:%t credential:%t model:%t", test.code, err.IsRequestScoped(), err.IsCredentialScoped(), err.IsModelScoped())
		}
	}
	risk := classifyHTTPError(http.StatusOK, []byte(`{"code":"4019"}`))
	if delay := risk.RetryAfter(); delay == nil || *delay <= 0 {
		t.Fatal("risk control error must carry a cooldown")
	}
}

func TestDerivedDeviceIdentifiersAreStableAndWellFormed(t *testing.T) {
	first := deriveDeviceID("1234567890123456789")
	if first != deriveDeviceID("1234567890123456789") {
		t.Fatal("device id is not deterministic")
	}
	if len(first) != 19 {
		t.Fatalf("device id length = %d", len(first))
	}
	for _, char := range first {
		if char < '0' || char > '9' {
			t.Fatalf("device id is not numeric: %q", first)
		}
	}
	machine := deriveMachineID("1234567890123456789")
	if len(machine) != 64 {
		t.Fatalf("machine id length = %d", len(machine))
	}
}

func TestReasoningBufferCoalescesShortDeltas(t *testing.T) {
	var flushed []string
	buffer := newReasoningBuffer(func(text string) bool {
		flushed = append(flushed, text)
		return true
	})
	for _, delta := range []string{"a", "b", "c"} {
		if !buffer.Push(delta) {
			t.Fatal("Push returned false")
		}
	}
	if len(flushed) != 0 {
		t.Fatalf("deltas were flushed too early: %#v", flushed)
	}
	buffer.Flush()
	if len(flushed) != 1 || flushed[0] != "abc" {
		t.Fatalf("flushed = %#v", flushed)
	}
}

// TestDeviceProofMatchesClientContract 校验客户端 pFe() 的关键契约：
// 换行拼接、秒级时间戳、32 位 hex nonce、ECDSA-P256 DER 签名、IDE_PC。
func TestDeviceProofMatchesClientContract(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	pemText := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	credential := Credential{RefreshToken: "refresh-token", UID: "1234567890123456789", DevicePrivateKey: pemText}
	info, proof, err := buildDeviceProof(IntlProfile, exchangeTokenPathV3, credential)
	if err != nil {
		t.Fatalf("buildDeviceProof: %v", err)
	}
	if info["PlatformCode"] != "IDE_PC" {
		t.Fatalf("platform code = %v", info["PlatformCode"])
	}
	nonce, _ := proof["Nonce"].(string)
	if len(nonce) != 32 {
		t.Fatalf("nonce length = %d", len(nonce))
	}
	timestamp, ok := proof["Timestamp"].(int64)
	if !ok || timestamp < 1_600_000_000 || timestamp > 4_000_000_000 {
		t.Fatalf("timestamp is not unix seconds: %#v", proof["Timestamp"])
	}
	canonical := strings.Join([]string{
		http.MethodPost, exchangeTokenPathV3, clientIDIDE, "refresh-token", strconv.FormatInt(timestamp, 10), nonce,
	}, "\n")
	signature, err := base64.StdEncoding.DecodeString(proof["Signature"].(string))
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	digest := sha256.Sum256([]byte(canonical))
	if !ecdsa.VerifyASN1(&key.PublicKey, digest[:], signature) {
		t.Fatal("signature does not verify as ECDSA-P256 DER over the newline canonical form")
	}
}

func TestParseStorageJSONPlaintextImport(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	auth, _ := json.Marshal(map[string]any{
		"token": "access-from-store", "refreshToken": "refresh-from-store",
		"userId": "1234567890123456789", "expiredAt": 1893456000000,
	})
	device, _ := json.Marshal(map[string]any{"privateKeyPEM": keyPEM, "publicKeyPEM": "pub"})
	store := map[string]any{
		"iCubeAuthInfo://icube.cloudide":  string(auth),
		"iCubeAuthInfo://icube-dc:987654321": string(device),
		"telemetry.machineId":             "machine-from-store",
	}
	raw, _ := json.Marshal(store)
	credential, err := ParseCredentialJSON(raw, IntlProfile)
	if err != nil {
		t.Fatalf("ParseCredentialJSON: %v", err)
	}
	if credential.AccessToken != "access-from-store" || credential.RefreshToken != "refresh-from-store" {
		t.Fatalf("tokens = %+v", credential)
	}
	if credential.UID != "1234567890123456789" || credential.DeviceID != "987654321" {
		t.Fatalf("identity = %+v", credential)
	}
	if credential.DevicePrivateKey == "" || credential.MachineID != "machine-from-store" {
		t.Fatalf("device material = %+v", credential)
	}
	if credential.ExpiresAt != 1893456000 {
		t.Fatalf("expires at = %d", credential.ExpiresAt)
	}
}

func TestParseStorageJSONRejectsTCEnvelope(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"iCubeAuthInfo://icube.cloudide": "dGMFEAAAsomeciphertext",
	})
	_, err := ParseCredentialJSON(raw, CNProfile)
	if !errors.Is(err, ErrEncryptedStorage) {
		t.Fatalf("err = %v, want ErrEncryptedStorage", err)
	}
}

func TestIntlImportRequiresDeviceKey(t *testing.T) {
	var driver = &driver{profile: IntlProfile}
	_, err := driver.ImportCredential(context.Background(), []byte(`{"realm":"intl","refresh_token":"r1"}`))
	if !errors.Is(err, ErrIntlMissingDeviceKey) {
		t.Fatalf("err = %v, want ErrIntlMissingDeviceKey", err)
	}
}

func TestMissingAccessTokenIsCredentialScoped(t *testing.T) {
	executor := NewExecutor(IntlProfile)
	_, err := executor.Execute(context.Background(), "cred", Credential{UID: "u1"}, ExecuteRequest{
		AttemptID: "a3", Model: "gpt-5.5", Format: "openai",
		Payload: []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hi"}]}`),
	})
	var upstream *UpstreamError
	if !errors.As(err, &upstream) {
		t.Fatalf("err = %v, want UpstreamError", err)
	}
	if upstream.ErrorCode() != "missing_access_token" || !upstream.IsCredentialScoped() {
		t.Fatalf("upstream error = %+v", upstream)
	}
	if !strings.Contains(upstream.Message, "access_token") {
		t.Fatalf("message = %q", upstream.Message)
	}
}
