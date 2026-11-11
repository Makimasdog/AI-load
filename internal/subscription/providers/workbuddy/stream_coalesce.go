package workbuddy

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// reasoningCoalesceMinRunes 是 reasoning 片段合并后下行所需的最小字符数。
	reasoningCoalesceMinRunes = 24
	// reasoningCoalesceInterval 兜底刷新间隔：上游逐 token 但生成较慢时，
	// 最多等待这么久就下发已缓存的 reasoning，避免看起来卡住。
	reasoningCoalesceInterval = 120 * time.Millisecond
)

// reasoningCoalescer 把上游逐 token 下发的 reasoning_content 合并成较大的
// 片段再转发。上游省略 token 边界信息，客户端（如 zcode）的思考面板会按
// chunk 逐条渲染，导致一个词一行；正文 content 不受影响。
type reasoningCoalescer struct {
	mu      sync.Mutex
	emit    func([]byte) bool
	started bool
	pending map[string]any
	text    string
	timer   *time.Timer
	closed  bool
}

func newReasoningCoalescer(emit func([]byte) bool) *reasoningCoalescer {
	return &reasoningCoalescer{emit: emit}
}

// Push 处理一个已归一化的 SSE chunk；返回 false 表示下游已不可写。
func (coalescer *reasoningCoalescer) Push(payload []byte) bool {
	chunk, ok := decodeStreamChunk(payload)
	if !ok {
		coalescer.mu.Lock()
		defer coalescer.mu.Unlock()
		coalescer.flushLocked()
		return coalescer.emit(payload)
	}
	text, mergeable := reasoningDelta(chunk)
	coalescer.mu.Lock()
	defer coalescer.mu.Unlock()
	if !mergeable {
		coalescer.flushLocked()
		return coalescer.emit(payload)
	}
	if !coalescer.started {
		// 首个 reasoning 片段直接下发，不额外增加首字延迟。
		coalescer.started = true
		return coalescer.emit(payload)
	}
	if coalescer.pending == nil {
		coalescer.pending = chunk
		coalescer.text = text
		coalescer.startTimerLocked()
		return true
	}
	coalescer.text += text
	if utf8.RuneCountInString(coalescer.text) >= reasoningCoalesceMinRunes || reasoningBoundary(text) {
		coalescer.flushLocked()
	}
	return true
}

// Close 下发残留的 reasoning 片段。
func (coalescer *reasoningCoalescer) Close() {
	coalescer.mu.Lock()
	defer coalescer.mu.Unlock()
	coalescer.flushLocked()
	coalescer.closed = true
}

func (coalescer *reasoningCoalescer) startTimerLocked() {
	if coalescer.timer != nil || coalescer.closed {
		return
	}
	coalescer.timer = time.AfterFunc(reasoningCoalesceInterval, func() {
		coalescer.mu.Lock()
		defer coalescer.mu.Unlock()
		coalescer.flushLocked()
	})
}

func (coalescer *reasoningCoalescer) flushLocked() {
	if coalescer.timer != nil {
		coalescer.timer.Stop()
		coalescer.timer = nil
	}
	chunk := coalescer.pending
	text := coalescer.text
	coalescer.pending = nil
	coalescer.text = ""
	if chunk == nil {
		return
	}
	choices, ok := chunk["choices"].([]any)
	if !ok || len(choices) == 0 {
		return
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return
	}
	delta, ok := choice["delta"].(map[string]any)
	if !ok {
		return
	}
	delta["reasoning_content"] = text
	payload, err := json.Marshal(chunk)
	if err != nil {
		return
	}
	coalescer.emit(payload)
}

func decodeStreamChunk(payload []byte) (map[string]any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var chunk map[string]any
	if err := decoder.Decode(&chunk); err != nil || chunk == nil {
		return nil, false
	}
	return chunk, true
}

// reasoningDelta 判断该 chunk 是否只携带 reasoning_content，返回待合并文本。
// 上游每个 chunk 都会带 content:""、function_call:null、tool_calls:[]、
// finish_reason:"" 这类空字段，只要它们没有实际内容就仍可合并。
func reasoningDelta(chunk map[string]any) (string, bool) {
	choices, ok := chunk["choices"].([]any)
	if !ok || len(choices) != 1 {
		return "", false
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return "", false
	}
	if !emptyValue(choice["finish_reason"]) {
		return "", false
	}
	delta, ok := choice["delta"].(map[string]any)
	if !ok {
		return "", false
	}
	text := ""
	for key, value := range delta {
		switch key {
		case "reasoning_content":
			typed, ok := value.(string)
			if !ok {
				return "", false
			}
			text = typed
		case "role":
		default:
			if !emptyValue(value) {
				return "", false
			}
		}
	}
	if text == "" {
		return "", false
	}
	return text, true
}

// emptyValue 判断上游占位字段是否为空（""、null、[]、{}）。
func emptyValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return typed == ""
	case []any:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	default:
		return false
	}
}

// reasoningBoundary 在句末或换行处提前刷新，避免缓存跨句累积。
func reasoningBoundary(text string) bool {
	trimmed := strings.TrimRight(text, " \t")
	if trimmed == "" {
		return false
	}
	return strings.HasSuffix(trimmed, ".") ||
		strings.HasSuffix(trimmed, "!") ||
		strings.HasSuffix(trimmed, "?") ||
		strings.HasSuffix(trimmed, ";") ||
		strings.HasSuffix(trimmed, "。") ||
		strings.HasSuffix(trimmed, "！") ||
		strings.HasSuffix(trimmed, "？") ||
		strings.HasSuffix(trimmed, "；") ||
		strings.HasSuffix(trimmed, "\n")
}
