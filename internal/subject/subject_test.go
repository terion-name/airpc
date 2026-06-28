package subject

import "testing"

func TestBuildSubjects(t *testing.T) {
	tests := []struct {
		name string
		fn   func(string) (string, error)
		want string
	}{
		{"unary", Unary, "airpc.v1.route.demo.unary"},
		{"open", Open, "airpc.v1.route.demo.open"},
		{"queue", ConnectorQueue, "airpc.route.demo.connectors"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn("demo")
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if got != tc.want {
				t.Fatalf("subject = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidateSubjectRejectsUnsafeSubjects(t *testing.T) {
	bad := []string{"", "foo..bar", "foo.*", "foo.>", "foo bar", "foo\nbar", ".foo", "foo."}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			if err := ValidateSubject(s); err == nil {
				t.Fatalf("ValidateSubject(%q) succeeded", s)
			}
		})
	}
	if err := ValidateSubject("airpc.v1.route.demo.unary"); err != nil {
		t.Fatalf("valid subject rejected: %v", err)
	}
}
