package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIValidate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"validate", "--config", filepath.Join("..", "..", "examples", "airpc.yaml")}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run validate: %v (stderr %q)", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "config ok") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestCLIRuntimeCommandsLoadConfigThenReturnNotWired(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "airpc.yaml")
	tests := [][]string{
		{"edge", "start", "--config", path},
		{"connector", "start", "--config", path, "--id", "local-1"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args[:2], " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(args, &stdout, &stderr)
			if !errors.Is(err, errRuntimeNotWired) {
				t.Fatalf("run(%v) error = %v, want runtime not wired", args, err)
			}
		})
	}
}
