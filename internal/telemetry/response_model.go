package telemetry

// knownResponseModelVariants maps a configured model name to the upstream
// variant names that are known to serve the same model. Antigravity answers
// Gemini 3.8 Flash requests with an internal tier name instead of the
// requested one, so those explicit pairs must not be reported as mismatches.
//
// Keep this list narrow: any other rename is still a real mismatch.
var knownResponseModelVariants = map[string]map[string]struct{}{
	"gemini-3.8-flash-low":    {"gemini-3.8-flash-n": {}},
	"gemini-3.8-flash-medium": {"gemini-3.8-flash-n": {}},
	"gemini-3.8-flash-high": {
		"gemini-3.8-flash-n": {},
		"gemini-3.8-flash-q": {},
	},
}

// ResponseModelEquivalent reports whether the upstream-reported model name is a
// known variant of the requested upstream model. Identical names always match.
func ResponseModelEquivalent(expected string, reported string) bool {
	if expected == reported {
		return true
	}
	variants, ok := knownResponseModelVariants[expected]
	if !ok {
		return false
	}
	_, ok = variants[reported]
	return ok
}
