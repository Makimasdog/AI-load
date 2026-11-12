package telemetry

import "testing"

func TestResponseModelEquivalent(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		reported string
		want     bool
	}{
		{name: "identical", expected: "gemini-3.8-flash-tiered", reported: "gemini-3.8-flash-tiered", want: true},
		{name: "low reports n", expected: "gemini-3.8-flash-low", reported: "gemini-3.8-flash-n", want: true},
		{name: "medium reports n", expected: "gemini-3.8-flash-medium", reported: "gemini-3.8-flash-n", want: true},
		{name: "high reports n", expected: "gemini-3.8-flash-high", reported: "gemini-3.8-flash-n", want: true},
		{name: "high reports q", expected: "gemini-3.8-flash-high", reported: "gemini-3.8-flash-q", want: true},
		{name: "low does not report q", expected: "gemini-3.8-flash-low", reported: "gemini-3.8-flash-q", want: false},
		{name: "medium does not report q", expected: "gemini-3.8-flash-medium", reported: "gemini-3.8-flash-q", want: false},
		{name: "tiered does not report n", expected: "gemini-3.8-flash-tiered", reported: "gemini-3.8-flash-n", want: false},
		{name: "unknown variant suffix", expected: "gemini-3.8-flash-high", reported: "gemini-3.8-flash-x", want: false},
		{name: "unrelated model", expected: "gpt-4o", reported: "gpt-4o-mini", want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := ResponseModelEquivalent(testCase.expected, testCase.reported); got != testCase.want {
				t.Fatalf("ResponseModelEquivalent(%q, %q) = %v, want %v",
					testCase.expected, testCase.reported, got, testCase.want)
			}
		})
	}
}
