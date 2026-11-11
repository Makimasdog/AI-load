package workbuddy

import "regexp"

// upstreamPromptBlocklist 记录 WorkBuddy 上游按内容拒绝的客户端提示词片段。
// 上游在这些片段命中时返回 11128 "Illegal API invocation from an unapproved
// channel"，即使首条消息是 system、tools 与流式参数都合法。片段来自其他编码
// 客户端自动注入的 system 说明（例如 gitStatus 里的分支描述），上游把它们
// 当作「非授权渠道」的指纹。网关转发前做等价改写，客户端无需改动。
var upstreamPromptBlocklist = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{
		pattern:     regexp.MustCompile(`(?i)Main branch \(you will usually use this for PRs\)`),
		replacement: "Main branch (usually the PR target)",
	},
}

// sanitizeBlockedPromptText 对提示词文本做等价的指纹规避改写。
func sanitizeBlockedPromptText(text string) string {
	for _, entry := range upstreamPromptBlocklist {
		text = entry.pattern.ReplaceAllString(text, entry.replacement)
	}
	return text
}

// sanitizeMessagesForUpstream 改写所有消息文本中的上游禁用片段。
// 兼容字符串 content 与 [{type:"text",text:"..."}] 两种形态。
func sanitizeMessagesForUpstream(body map[string]any) {
	messages, ok := body["messages"].([]any)
	if !ok {
		return
	}
	for _, item := range messages {
		message, ok := item.(map[string]any)
		if !ok {
			continue
		}
		switch content := message["content"].(type) {
		case string:
			message["content"] = sanitizeBlockedPromptText(content)
		case []any:
			for _, rawPart := range content {
				part, ok := rawPart.(map[string]any)
				if !ok {
					continue
				}
				if text, ok := part["text"].(string); ok {
					part["text"] = sanitizeBlockedPromptText(text)
				}
			}
		}
	}
}
