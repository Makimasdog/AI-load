package cpa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/protocol"
	"gpt-load/internal/subscription/providers/trae"
)

type traeProviderCredential struct{ value trae.Credential }

func (credential traeProviderCredential) redactionValues() []string {
	return credential.value.SecretValues()
}

// traeProviderBridge 把 Trae 的两个区域共用同一份执行逻辑，只在路由断言与
// 错误文案上使用 profile 区分。
type traeProviderBridge struct {
	profile  trae.Profile
	executor trae.Executor
}

func newTraeProviderBridge(profile trae.Profile) *traeProviderBridge {
	return &traeProviderBridge{profile: profile, executor: trae.NewExecutor(profile)}
}

func (bridge *traeProviderBridge) ProviderKind() channel.ProviderKind {
	if bridge != nil && bridge.profile.Key == "intl" {
		return channel.ProviderTraeIntl
	}
	return channel.ProviderTrae
}

func (*traeProviderBridge) UpstreamProtocol() protocol.Protocol { return protocol.OpenAICompletions }

func (*traeProviderBridge) ValidateRouteCapability(route channel.RouteDescriptor) error {
	if route.ClientProtocol == protocol.OpenAICompletions &&
		(route.Operation == execution.OperationChatCompletion || route.Operation == execution.OperationProbe) &&
		route.RouteMode == execution.RouteNative {
		return nil
	}
	return fmt.Errorf("route is not implemented by Trae")
}

func (bridge *traeProviderBridge) ParseCredential(raw []byte) (providerCredential, error) {
	credential, err := trae.ParseCredentialJSON(raw, bridge.profile)
	if err != nil {
		return nil, err
	}
	return traeProviderCredential{value: credential}, nil
}

func (bridge *traeProviderBridge) Execute(
	ctx context.Context,
	credentialID string,
	credential providerCredential,
	request providerRequest,
) (providerResponse, error) {
	value, ok := credential.(traeProviderCredential)
	if !ok || bridge == nil || bridge.executor == nil {
		return providerResponse{}, errors.New("Trae provider bridge credential mismatch")
	}
	response, err := bridge.executor.Execute(ctx, credentialID, value.value, traeRequest(request))
	if err != nil {
		return providerResponse{}, err
	}
	return providerResponse{
		Payload: append([]byte(nil), response.Payload...), Headers: response.Headers.Clone(),
		UpstreamProtocol: protocol.OpenAICompletions,
	}, nil
}

func (bridge *traeProviderBridge) ExecuteStream(
	ctx context.Context,
	credentialID string,
	credential providerCredential,
	request providerRequest,
) (*providerStreamResponse, error) {
	value, ok := credential.(traeProviderCredential)
	if !ok || bridge == nil || bridge.executor == nil {
		return nil, errors.New("Trae provider bridge credential mismatch")
	}
	response, err := bridge.executor.ExecuteStream(ctx, credentialID, value.value, traeRequest(request))
	if response == nil {
		return nil, err
	}
	if err != nil {
		return &providerStreamResponse{Headers: response.Headers.Clone(), UpstreamProtocol: protocol.OpenAICompletions}, err
	}
	chunks := make(chan providerStreamChunk)
	go func() {
		defer close(chunks)
		for chunk := range response.Chunks {
			converted := providerStreamChunk{
				Payload: append([]byte(nil), chunk.Payload...), Incomplete: chunk.Incomplete, Err: chunk.Err,
			}
			select {
			case chunks <- converted:
			case <-ctx.Done():
				return
			}
		}
	}()
	return &providerStreamResponse{
		Headers: response.Headers.Clone(), Chunks: chunks, UpstreamProtocol: protocol.OpenAICompletions,
	}, nil
}

func traeRequest(request providerRequest) trae.ExecuteRequest {
	payload := request.Payload
	if len(bytes.TrimSpace(payload)) == 0 {
		// 凭据探测没有请求体：发一次最小真实补全，让结果反映上游状态。
		payload = traeProbePayload(request.Model)
	}
	return trae.ExecuteRequest{
		AttemptID: request.AttemptID, Model: request.Model,
		Payload: append([]byte(nil), payload...), Format: request.Format,
		Headers: request.Headers.Clone(), OriginalRequest: append([]byte(nil), request.OriginalRequest...),
		ContinuityKey: request.ContinuityKey, BaseURL: request.BaseURL, ProxyURL: request.ProxyURL,
		ProxyFromEnvironment: request.ProxyFromEnvironment,
		ConfiguredHeaders:    append([]string(nil), request.ConfiguredHeaders...),
	}
}

func (*traeProviderBridge) ClassifyError(
	ctx context.Context,
	err error,
	credential providerCredential,
) (int, *execution.ErrorEvidence) {
	if err == nil {
		return 0, nil
	}
	status := 0
	var statusError interface{ StatusCode() int }
	if errors.As(err, &statusError) && statusError != nil {
		status = statusError.StatusCode()
	}
	kind := execution.ErrorKindTransport
	if errors.Is(err, context.DeadlineExceeded) || ctx != nil && errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		kind = execution.ErrorKindTimeout
	} else if errors.Is(err, context.Canceled) || ctx != nil && errors.Is(context.Cause(ctx), context.Canceled) {
		kind = execution.ErrorKindCanceled
	} else if status != 0 {
		kind = execution.ErrorKindHTTP
	} else if _, ok := err.(net.Error); ok {
		kind = execution.ErrorKindTransport
	}
	code := ""
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) && coded != nil {
		code = strings.TrimSpace(coded.ErrorCode())
	}
	upstreamMessage := ""
	var upstream *trae.UpstreamError
	hasUpstream := errors.As(err, &upstream) && upstream != nil
	if hasUpstream {
		upstreamMessage = upstream.Message
	}
	evidence := &execution.ErrorEvidence{
		Kind: kind, StatusCode: status, Code: code,
		Summary: traeErrorSummary(status, code, upstreamMessage),
	}
	var retry interface{ RetryAfter() *time.Duration }
	if errors.As(err, &retry) && retry != nil {
		if value := retry.RetryAfter(); value != nil && *value > 0 {
			evidence.RetryAfter = *value
		}
	}
	switch {
	case status == http.StatusUnauthorized || code == "1001" || code == "20101":
		evidence.Hint = execution.FailureHintRefreshRequired
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
	case hasUpstream && upstream.Model:
		evidence.Hint = execution.FailureHintModelUnavailable
	case code == "4019":
		// 风控命中：整账号冷却，禁止换号重试。
		evidence.Hint = execution.FailureHintRateLimited
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
		evidence.ScopeHint = execution.ErrorScopeCredential
	case hasUpstream && upstream.Request:
		evidence.Hint = execution.FailureHintRequestRejected
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
	case hasUpstream && upstream.Credential:
		evidence.Hint = execution.FailureHintRateLimited
	case status == http.StatusForbidden:
		// 4015/4011 属于上游风控或版本差异，不惩罚账号。
		evidence.Hint = execution.FailureHintHostError
	case status == http.StatusPaymentRequired:
		evidence.Hint = execution.FailureHintCandidateUnavailable
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
	case status == http.StatusTooManyRequests || code == "4011":
		evidence.Hint = execution.FailureHintRateLimited
	case status >= http.StatusInternalServerError:
		evidence.Hint = execution.FailureHintHostError
	}
	if evidence.Summary == "" {
		evidence.Summary = safeErrorSummary(err, credential.redactionValues())
	}
	annotateProviderErrorEvidence(evidence, err)
	if hasUpstream && upstream.Model {
		evidence.ScopeHint = execution.ErrorScopeModel
	}
	return status, evidence
}

func traeProbePayload(model string) []byte {
	body, err := json.Marshal(map[string]any{
		"model": strings.TrimSpace(model),
		"messages": []map[string]string{
			{"role": "user", "content": "ping"},
		},
		"max_tokens": 16,
		"stream":     false,
	})
	if err != nil {
		return []byte(`{"messages":[{"role":"user","content":"ping"}],"max_tokens":16,"stream":false}`)
	}
	return body
}

func traeErrorSummary(status int, code, upstreamMessage string) string {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "1001", "20101":
		return "登录已失效，请重新导入该账号的 Trae 凭证"
	case "4001":
		if upstreamMessage != "" {
			return "上游 code=4001（模型标识或参数不被接受）：" + upstreamMessage
		}
		return "请求被上游拒绝：消息格式或模型名不合法（code=4001）"
	case "3003":
		return "上游提示词管线处理失败，请检查消息内容"
	case "4011":
		return "上游把该请求识别为异常流量，请稍后重试"
	case "4015":
		return "该账号无权使用此模型，或国际版需要与账号区域一致的出口"
	case "4019":
		return "账号触发上游风控，已暂停该账号，请稍后重试"
	case "4023":
		if upstreamMessage != "" {
			return "上游 code=4023：" + upstreamMessage
		}
		return "上游不支持或已下线该模型（code=4023）"
	case "1005":
		if upstreamMessage != "" {
			return "上游 code=1005（extra=" + upstreamMessage + "）：当前账号套餐不支持该模型配置"
		}
		return "上游 code=1005：当前账号套餐不支持该模型配置"
	case "20403", "20405":
		return "设备指纹校验失败，请重新导入包含设备信息的凭证"
	case "missing_access_token":
		return "该 Trae 凭证缺少 access_token，请重新导入或先执行刷新"
	}
	switch {
	case status == http.StatusUnauthorized:
		return "登录已失效，请重新导入该账号的 Trae 凭证"
	case status == http.StatusForbidden:
		return "上游拒绝了该请求：模型权限或区域限制"
	case status == http.StatusTooManyRequests:
		return "上游限流，请稍后重试"
	case status >= http.StatusInternalServerError:
		return "Trae upstream failed to process the request"
	default:
		return ""
	}
}
