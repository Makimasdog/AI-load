package workbuddy

import "testing"

func TestQuotaWindowStateUsesContractValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		total float64
		left  float64
		want  string
	}{
		{name: "remaining capacity", total: 100, left: 50, want: "available"},
		{name: "zero capacity", total: 0, left: 0, want: "available"},
		{name: "fully consumed", total: 100, left: 0, want: "exhausted"},
		{name: "overdrawn", total: 100, left: -1, want: "exhausted"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := quotaWindowState(test.total, test.left)
			if got != test.want {
				t.Fatalf("quotaWindowState(%v, %v) = %q, want %q", test.total, test.left, got, test.want)
			}
			if got != "available" && got != "exhausted" {
				t.Fatalf("quotaWindowState(%v, %v) = %q, outside the available|exhausted contract", test.total, test.left, got)
			}
		})
	}
}
