package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIRuntimeCommandsLoadConfigAndPrintPlan(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "airpc.yaml")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "edge", args: []string{"edge", "start", "--config", path}, want: "would start edge"},
		{name: "connector", args: []string{"connector", "start", "--config", path, "--id", "local-1"}, want: "would start connector"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(tc.args, &stdout, &stderr)
			if err != nil {
				t.Fatalf("run(%v): %v (stderr %q)", tc.args, err, stderr.String())
			}
			if !strings.Contains(stdout.String(), tc.want) {
				t.Fatalf("stdout = %q, want %q", stdout.String(), tc.want)
			}
		})
	}
}

func TestCLIMissingArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "no command", args: nil, want: "missing command"},
		{name: "edge missing start", args: []string{"edge"}, want: "missing edge command"},
		{name: "edge missing config", args: []string{"edge", "start"}, want: "--config"},
		{name: "connector missing id", args: []string{"connector", "start", "--config", filepath.Join("..", "..", "examples", "airpc.yaml")}, want: "--id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(tc.args, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) error = %v, want containing %q", tc.args, err, tc.want)
			}
		})
	}
}
