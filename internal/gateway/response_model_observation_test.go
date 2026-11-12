package gateway

import (
	"testing"

	"gpt-load/internal/dialect"
)

func TestResponseModelMatchesKnownGeminiVariants(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		reported string
		want     bool
	}{
		{name: "low reports n", expected: "gemini-3.8-flash-low", reported: "gemini-3.8-flash-n", want: true},
		{name: "medium reports n", expected: "gemini-3.8-flash-medium", reported: "gemini-3.8-flash-n", want: true},
		{name: "high reports n", expected: "gemini-3.8-flash-high", reported: "gemini-3.8-flash-n", want: true},
		{name: "high reports q", expected: "gemini-3.8-flash-high", reported: "gemini-3.8-flash-q", want: true},
		{name: "identical", expected: "gemini-3.8-flash-tiered", reported: "gemini-3.8-flash-tiered", want: true},
		{name: "low does not report q", expected: "gemini-3.8-flash-low", reported: "gemini-3.8-flash-q", want: false},
		{name: "medium does not report q", expected: "gemini-3.8-flash-medium", reported: "gemini-3.8-flash-q", want: false},
		{name: "tiered does not report n", expected: "gemini-3.8-flash-tiered", reported: "gemini-3.8-flash-n", want: false},
		{name: "unknown variant suffix", expected: "gemini-3.8-flash-low", reported: "gemini-3.8-flash-x", want: false},
		{name: "unrelated model", expected: "gpt-4o", reported: "gpt-4o-mini", want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := responseModelMatches(testCase.expected, testCase.reported); got != testCase.want {
				t.Fatalf("responseModelMatches(%q, %q) = %v, want %v",
					testCase.expected, testCase.reported, got, testCase.want)
			}
		})
	}
}

func TestResponseModelTrackerTreatsKnownGeminiVariantAsMatch(t *testing.T) {
	tracker := newResponseModelTracker(dialect.NewOpenAI(), "gemini-3.8-flash-low")
	tracker.observe([]byte(`{"model":"gemini-3.8-flash-n","choices":[]}`))

	observation := tracker.observation()
	if !observation.observed || observation.mismatch {
		t.Fatalf("observation = %+v, want observed match", observation)
	}
	if observation.reported != "gemini-3.8-flash-n" {
		t.Fatalf("reported = %q, want the real upstream name", observation.reported)
	}
}

func TestResponseModelTrackerKeepsUnrelatedMismatch(t *testing.T) {
	tracker := newResponseModelTracker(dialect.NewOpenAI(), "gpt-4o")
	tracker.observe([]byte(`{"model":"gpt-4o-mini","choices":[]}`))

	observation := tracker.observation()
	if !observation.observed || !observation.mismatch {
		t.Fatalf("observation = %+v, want observed mismatch", observation)
	}
	if observation.reported != "gpt-4o-mini" {
		t.Fatalf("reported = %q, want gpt-4o-mini", observation.reported)
	}
}
