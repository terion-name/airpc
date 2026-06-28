package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/terion-name/airpc/internal/config"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("missing command (expected edge start or connector start)")
	}

	switch args[0] {
	case "edge":
		return runEdge(args[1:], stdout, stderr)
	case "connector":
		return runConnector(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q (expected edge or connector)", args[0])
	}
}

func runEdge(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("missing edge command (expected start)")
	}
	if args[0] != "start" {
		return fmt.Errorf("unknown edge command %q (expected start)", args[0])
	}

	fs := flag.NewFlagSet("airpc edge start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to airpc YAML config")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("edge start does not accept positional arguments")
	}
	if *configPath == "" {
		return fmt.Errorf("edge start requires --config <path>")
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "would start edge: config=%s http_addr=%s data_addr=%s routes=%d\n", *configPath, cfg.Edge.HTTPAddr, cfg.Edge.DataAddr, len(cfg.Routes))
	return nil
}

func runConnector(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("missing connector command (expected start)")
	}
	if args[0] != "start" {
		return fmt.Errorf("unknown connector command %q (expected start)", args[0])
	}

	fs := flag.NewFlagSet("airpc connector start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to airpc YAML config")
	connectorID := fs.String("id", "", "connector id")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("connector start does not accept positional arguments")
	}
	if *configPath == "" {
		return fmt.Errorf("connector start requires --config <path>")
	}
	if *connectorID == "" {
		return fmt.Errorf("connector start requires --id <id>")
	}
	if err := config.ValidateSubjectToken("connector id", *connectorID); err != nil {
		return err
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "would start connector: id=%s config=%s edge_data_url=%s routes=%d\n", *connectorID, *configPath, cfg.Connector.EdgeDataURL, len(cfg.Routes))
	return nil
}
