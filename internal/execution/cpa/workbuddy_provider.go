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
	"gpt-load/internal/subscription/providers/workbuddy"
)

type workBuddyProviderCredential struct{ value workbuddy.Credential }

func (credential workBuddyProviderCredential) redactionValues() []string {
	return credential.value.SecretValues()
}

type workBuddyProviderBridge struct{ executor workbuddy.Executor }

func newWorkBuddyProviderBridge() *workBuddyProviderBridge {
	return &workBuddyProviderBridge{executor: workbuddy.NewExecutor()}
}

func (*workBuddyProviderBridge) ProviderKind() channel.ProviderKind { return channel.ProviderWorkBuddy }

func (*workBuddyProviderBridge) UpstreamProtocol() protocol.Protocol {
	return protocol.OpenAICompletions
}

func (*workBuddyProviderBridge) ValidateRouteCapability(route channel.RouteDescriptor) error {
	if route.ClientProtocol == protocol.OpenAICompletions &&
		(route.Operation == execution.OperationChatCompletion || route.Operation == execution.OperationProbe) &&
		route.RouteMode == execution.RouteNative {
		return nil
	}
	return fmt.Errorf("route is not implemented by WorkBuddy")
}

func (*workBuddyProviderBridge) ParseCredential(raw []byte) (providerCredential, error) {
	credential, err := workbuddy.ParseCredentialJSON(raw)
	if err != nil {
		return nil, err
	}
	return workBuddyProviderCredential{value: credential}, nil
}

func (bridge *workBuddyProviderBridge) Execute(
	ctx context.Context,
	credentialID string,
	credential providerCredential,
	request providerRequest,
) (providerResponse, error) {
	value, ok := credential.(workBuddyProviderCredential)
	if !ok || bridge == nil || bridge.executor == nil {
		return providerResponse{}, errors.New("WorkBuddy provider bridge credential mismatch")
	}
	response, err := bridge.executor.Execute(ctx, credentialID, value.value, workBuddyRequest(request))
	if err != nil {
		return providerResponse{}, err
	}
	return providerResponse{
		Payload: append([]byte(nil), response.Payload...), Headers: response.Headers.Clone(),
		UpstreamProtocol: protocol.OpenAICompletions,
	}, nil
}

func (bridge *workBuddyProviderBridge) ExecuteStream(
	ctx context.Context,
	credentialID string,
	credential providerCredential,
	request providerRequest,
) (*providerStreamResponse, error) {
	value, ok := credential.(workBuddyProviderCredential)
	if !ok || bridge == nil || bridge.executor == nil {
		return nil, errors.New("WorkBuddy provider bridge credential mismatch")
	}
	response, err := bridge.executor.ExecuteStream(ctx, credentialID, value.value, workBuddyRequest(request))
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

func workBuddyRequest(request providerRequest) workbuddy.ExecuteRequest {
	payload := request.Payload
	if len(bytes.TrimSpace(payload)) == 0 {
		// A credential probe carries no body; send the smallest real chat
		// completion so the result reflects the upstream instead of a
		// validation error.
		payload = probePayload(request.Model)
	}
	return workbuddy.ExecuteRequest{
		AttemptID: request.AttemptID, Model: request.Model,
		Payload: append([]byte(nil), payload...), Format: request.Format,
		Headers: request.Headers.Clone(), OriginalRequest: append([]byte(nil), request.OriginalRequest...),
		ContinuityKey: request.ContinuityKey, BaseURL: request.BaseURL, ProxyURL: request.ProxyURL,
		ProxyFromEnvironment: request.ProxyFromEnvironment,
		ConfiguredHeaders:    append([]string(nil), request.ConfiguredHeaders...),
	}
}

func (*workBuddyProviderBridge) ClassifyError(
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
	var upstream *workbuddy.UpstreamError
	hasUpstream := errors.As(err, &upstream) && upstream != nil
	if hasUpstream {
		upstreamMessage = upstream.Message
	}
	evidence := &execution.ErrorEvidence{
		Kind: kind, StatusCode: status, Code: code,
		Summary: workBuddyErrorSummary(status, code, upstreamMessage),
	}
	var retry interface{ RetryAfter() *time.Duration }
	if errors.As(err, &retry) && retry != nil {
		if value := retry.RetryAfter(); value != nil && *value > 0 {
			evidence.RetryAfter = *value
		}
	}
	switch {
	case status == http.StatusUnauthorized || code == "12153":
		evidence.Hint = execution.FailureHintRefreshRequired
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
	case hasUpstream && upstream.Model:
		evidence.Hint = execution.FailureHintModelUnavailable
	case hasUpstream && upstream.Request:
		evidence.Hint = execution.FailureHintRequestRejected
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
	case hasUpstream && upstream.Credential:
		evidence.Hint = execution.FailureHintRateLimited
	case status == http.StatusForbidden:
		if hasUpstream && code == "11140" {
			evidence.Hint = execution.FailureHintRateLimited
			evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
		} else {
			// 11128/HTML WAF：不惩罚账号，按上游主机错误重试。
			evidence.Hint = execution.FailureHintHostError
		}
	case status == http.StatusPaymentRequired:
		evidence.Hint = execution.FailureHintCandidateUnavailable
		evidence.ReplaySafety = execution.ReplaySafetyRejectedBeforeProcessing
	case status == http.StatusTooManyRequests || code == "6004" || code == "14017" || code == "14018":
		evidence.Hint = execution.FailureHintRateLimited
	case status >= http.StatusInternalServerError || code == "6021" || code == "11134":
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

// probePayload builds the minimal chat completion a credential probe sends.
func probePayload(model string) []byte {
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

func workBuddyErrorSummary(status int, code, upstreamMessage string) string {
	switch {
	case status == http.StatusUnauthorized || code == "12153":
		return "登录已失效，请在该账号上重新登录"
	case code == "6004":
		return "该模型触发上游频率限制，请稍后重试"
	case code == "14018":
		return "该模型的积分已用完，请切换模型或等待额度刷新"
	case code == "14017":
		return "账号试用额度未激活，请先在上游领取"
	case code == "11133":
		return "该账号无权使用此模型"
	case code == "11102", code == "11158":
		return "上游不支持或已下线该模型"
	case code == "11101", code == "11115", code == "11135":
		return "请求被上游拒绝，请检查模型参数"
	case code == "11128":
		if detail := upstreamDetail(upstreamMessage); detail != "" {
			return "上游拒绝了该请求（" + detail + "）"
		}
		return "上游拒绝了该请求：首条消息必须为 system，或内容触发了安全策略"
	case code == "6021":
		return "WorkBuddy upstream queue is full"
	case status == http.StatusForbidden:
		return "WorkBuddy access was denied by upstream risk control"
	case status == http.StatusTooManyRequests:
		return "WorkBuddy upstream rate limited the request"
	case status == http.StatusBadRequest && upstreamDetail(upstreamMessage) != "":
		return "上游拒绝了该请求（" + upstreamDetail(upstreamMessage) + "）"
	case status >= http.StatusInternalServerError:
		return "WorkBuddy upstream failed to process the request"
	default:
		return ""
	}
}

// upstreamDetail 把上游错误文案裁剪成可读的一小段，避免把整段响应塞给客户端。
func upstreamDetail(message string) string {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) > 160 {
		trimmed = string(runes[:160]) + "…"
	}
	return trimmed
}
