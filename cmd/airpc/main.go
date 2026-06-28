package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/terion-name/airpc/internal/config"
)

var errRuntimeNotWired = errors.New("airpc runtime is not wired yet")

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("airpc", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "airpc.yaml", "path to airpc YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cmd := "validate"
	if fs.NArg() > 0 {
		cmd = fs.Arg(0)
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}

	switch cmd {
	case "validate":
		fmt.Fprintf(os.Stdout, "config ok: %d route(s) for connector %q\n", len(cfg.Routes), cfg.Connector.ID)
		return nil
	case "edge", "connector":
		return fmt.Errorf("%s: %w", cmd, errRuntimeNotWired)
	default:
		return fmt.Errorf("unknown command %q (expected validate, edge, or connector)", cmd)
	}
}
