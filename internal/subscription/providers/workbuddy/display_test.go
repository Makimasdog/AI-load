package workbuddy

import (
	"testing"

	"gpt-load/internal/subscription/providers/observation"
)

func TestDisplayLabelPrefersNicknameThenEmailThenPhoneThenUID(t *testing.T) {
	cases := []struct {
		value Credential
		want  string
	}{
		{Credential{Nickname: "小楚", Email: "a@b.com", Phone: "13800000000", UID: "uid-123456789"}, "小楚"},
		{Credential{Email: "a@b.com", Phone: "13800000000", UID: "uid-123456789"}, "a@b.com"},
		{Credential{Phone: "13800000000", UID: "uid-123456789"}, "13800000000"},
		{Credential{UID: "uid-123456789"}, "WorkBuddy uid-1234"},
		{Credential{}, "WorkBuddy"},
	}
	for _, test := range cases {
		if got := test.value.DisplayLabel(); got != test.want {
			t.Fatalf("DisplayLabel(%+v) = %q, want %q", test.value, got, test.want)
		}
	}
}

func TestDecodeAccountPayloadAcceptsAccountsWrapper(t *testing.T) {
	account, err := decodeAccountPayload([]byte(`{"accounts":[{"uid":"u1","nickname":"小楚","email":"a@b.com","phoneNumber":"13800000000"}]}`))
	if err != nil {
		t.Fatalf("decode account: %v", err)
	}
	if account.UID != "u1" || account.nickname() != "小楚" || account.Email != "a@b.com" || account.phone() != "13800000000" {
		t.Fatalf("account = %+v", account)
	}
}

func TestCatalogEntriesDropsAliasesAndServiceModels(t *testing.T) {
	models := []catalogModel{
		{ID: "hy4-preview", Name: "混元 4 预览版", MaxOutputTokens: 64000},
		{ID: "glm-5.2", Name: "GLM 5.2", MaxOutputTokens: 32000},
		{ID: "default-model", Name: "Default"},
		{ID: "fast-model", Name: "Fast"},
		{ID: "nes-1.2", Name: "NES"},
		{ID: "completion-1.0", Name: "Completion"},
		{ID: "seedance-2.5", Name: "Seedance", Tags: []string{"text-to-image"}},
		{ID: "disabled-model", Disabled: true},
	}
	allowed := map[string]struct{}{"hy4-preview": {}, "glm-5.2": {}, "default-model": {}, "seedance-2.5": {}}
	entries := catalogEntries(models, allowed)
	if len(entries) != 2 || entries[0].ID != "hy4-preview" || entries[1].ID != "glm-5.2" {
		t.Fatalf("entries = %+v", entries)
	}
	if entries[0].Name != "混元 4 预览版" {
		t.Fatalf("display name = %q", entries[0].Name)
	}
}

func TestMergeCatalogEntriesKeepsConfigOrderAndAddsEnterpriseModels(t *testing.T) {
	merged := mergeCatalogEntries(
		[]modelEntry{{ID: "a", Name: "A"}, {ID: "b", Name: "B"}},
		[]modelEntry{{ID: "b", Name: "B2"}, {ID: "c", Name: "C"}},
	)
	if len(merged) != 3 || merged[0].ID != "a" || merged[1].ID != "b" || merged[2].ID != "c" {
		t.Fatalf("merged = %+v", merged)
	}
	if merged[1].Name != "B" {
		t.Fatalf("config entry should win: %+v", merged[1])
	}
}

func TestFormatCreditsAndPlanLevel(t *testing.T) {
	if got := formatCredits(665.256); got != "665.26" {
		t.Fatalf("formatCredits = %q", got)
	}
	if got := formatCredits(100); got != "100" {
		t.Fatalf("formatCredits = %q", got)
	}
	if planLevel("free", false) != observation.PlanLevelFree {
		t.Fatal("free plan level")
	}
	if planLevel("pro", true) != observation.PlanLevelPremium {
		t.Fatal("pro plan level")
	}
}
