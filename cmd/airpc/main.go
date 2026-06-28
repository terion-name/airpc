package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/terion-name/airpc/internal/config"
)

var errRuntimeNotWired = errors.New("airpc runtime is not wired yet")

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		args = []string{"validate"}
	}
	switch args[0] {
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "edge":
		return runEdge(args[1:], stderr)
	case "connector":
		return runConnector(args[1:], stderr)
	default:
		return fmt.Errorf("unknown command %q (expected validate, edge, or connector)", args[0])
	}
}

func runValidate(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("airpc validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "airpc.yaml", "path to airpc YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("validate does not accept positional arguments")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "config ok: %d edge route(s), %d connector route(s), connector %q\n", len(cfg.Edge.Routes), len(cfg.Connector.Routes), cfg.Connector.ID)
	return nil
}

func runEdge(args []string, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "start" {
		return fmt.Errorf("unknown edge command (expected start)")
	}
	fs := flag.NewFlagSet("airpc edge start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "airpc.yaml", "path to airpc YAML config")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("edge start does not accept positional arguments")
	}
	if _, err := config.LoadFile(*configPath); err != nil {
		return err
	}
	return fmt.Errorf("edge start: %w", errRuntimeNotWired)
}

func runConnector(args []string, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "start" {
		return fmt.Errorf("unknown connector command (expected start)")
	}
	fs := flag.NewFlagSet("airpc connector start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "airpc.yaml", "path to airpc YAML config")
	connectorID := fs.String("id", "", "connector id override")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("connector start does not accept positional arguments")
	}
	if _, err := config.LoadFileWithConnectorID(*configPath, *connectorID); err != nil {
		return err
	}
	return fmt.Errorf("connector start: %w", errRuntimeNotWired)
}
