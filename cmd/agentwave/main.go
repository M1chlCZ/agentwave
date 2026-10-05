// Command agentwave serves an embeddable agent assistant on a private Unix
// socket. It reads a JSON policy, builds the Codex CLI runner and exposes the
// suggestion API for a host application to proxy.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/m1chlcz/agentwave"
	"github.com/m1chlcz/agentwave/codex"
	"github.com/m1chlcz/agentwave/server"
)

const version = "0.1.0"

const shutdownTimeout = 10 * time.Second

const (
	exitSuccess = 0
	exitFailure = 1
	exitUsage   = 2
)

type options struct {
	socket  string
	policy  string
	model   string
	codex   string
	home    string
	version bool
}

type policyDocument struct {
	Actions      []string          `json:"actions"`
	Modules      []string          `json:"modules"`
	RoutePrefix  string            `json:"routePrefix"`
	QueryActions map[string]string `json:"queryActions"`
	Working      string            `json:"working"`
	Failed       string            `json:"failed"`
}

func (document policyDocument) policy() agentwave.Policy {
	return agentwave.Policy{
		Actions:      document.Actions,
		Modules:      document.Modules,
		RoutePrefix:  document.RoutePrefix,
		QueryActions: document.QueryActions,
		Working:      document.Working,
		Failed:       document.Failed,
	}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	parsed, err := parseFlags(args, stderr)
	if err != nil {
		return exitUsage
	}
	if parsed.version {
		fmt.Fprintln(stdout, version)
		return exitSuccess
	}
	if code := validateOptions(parsed, stderr); code != exitSuccess {
		return code
	}
	policy, err := loadPolicy(parsed.policy)
	if err != nil {
		fmt.Fprintf(stderr, "agentwave: %v\n", err)
		return exitUsage
	}
	runner, err := codex.New(codex.Config{Binary: parsed.codex, Model: parsed.model, Home: parsed.home, Policy: policy})
	if err != nil {
		fmt.Fprintf(stderr, "agentwave: %v\n", err)
		return exitUsage
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := agentwave.NewStore(ctx, policy, runner.Generate)
	if err != nil {
		fmt.Fprintf(stderr, "agentwave: %v\n", err)
		return exitUsage
	}
	defer store.Close()
	return serve(ctx, parsed, store, stderr)
}

func validateOptions(parsed options, stderr io.Writer) int {
	switch {
	case parsed.socket == "":
		fmt.Fprintln(stderr, "agentwave: --socket is required")
	case !filepath.IsAbs(parsed.socket):
		fmt.Fprintln(stderr, "agentwave: --socket must be an absolute path")
	case parsed.policy == "":
		fmt.Fprintln(stderr, "agentwave: --policy is required")
	case parsed.model == "":
		fmt.Fprintln(stderr, "agentwave: --model is required")
	default:
		return exitSuccess
	}
	return exitUsage
}

func serve(ctx context.Context, parsed options, store *agentwave.Store, stderr io.Writer) int {
	listener, err := server.ListenUnix(parsed.socket)
	if err != nil {
		fmt.Fprintf(stderr, "agentwave: listen on %s: %v\n", parsed.socket, err)
		return exitFailure
	}
	created, err := os.Lstat(parsed.socket)
	if err != nil {
		_ = listener.Close()
		fmt.Fprintf(stderr, "agentwave: inspect socket %s: %v\n", parsed.socket, err)
		return exitFailure
	}
	logger := log.New(stderr, "agentwave: ", log.LstdFlags)
	service := &http.Server{Handler: server.New(store), ReadHeaderTimeout: shutdownTimeout}
	served := make(chan error, 1)
	go func() { served <- service.Serve(listener) }()
	logger.Printf("listening on %s with model %s", parsed.socket, parsed.model)
	code := exitSuccess
	select {
	case err = <-served:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("server stopped: %v", err)
			code = exitFailure
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		if err = service.Shutdown(shutdownCtx); err != nil {
			_ = service.Close()
		}
		cancel()
	}
	store.Close()
	removeOwnedSocket(parsed.socket, created)
	return code
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	flags := flag.NewFlagSet("agentwave", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(
			stderr,
			"usage: agentwave --socket PATH --policy PATH --model NAME [--codex PATH] [--home DIR] [--version]",
		)
	}
	var parsed options
	flags.StringVar(&parsed.socket, "socket", "", "absolute path of the Unix socket (required)")
	flags.StringVar(&parsed.policy, "policy", "", "path of the JSON policy file (required)")
	flags.StringVar(&parsed.model, "model", "", "model slug (required)")
	flags.StringVar(&parsed.codex, "codex", "codex", "path of the Codex CLI executable")
	flags.StringVar(&parsed.home, "home", "", "home directory of the sandboxed CLI")
	flags.BoolVar(&parsed.version, "version", false, "print the version and exit")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "agentwave: unexpected argument %q\n", flags.Arg(0))
		return options{}, errors.New("unexpected argument")
	}
	return parsed, nil
}

func loadPolicy(path string) (agentwave.Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return agentwave.Policy{}, fmt.Errorf("read policy %s: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document policyDocument
	if decodeErr := decoder.Decode(&document); decodeErr != nil {
		return agentwave.Policy{}, fmt.Errorf("decode policy %s: %w", path, decodeErr)
	}
	if _, tokenErr := decoder.Token(); !errors.Is(tokenErr, io.EOF) {
		return agentwave.Policy{}, fmt.Errorf("decode policy %s: trailing content", path)
	}
	policy := document.policy()
	if validateErr := policy.Validate(); validateErr != nil {
		return agentwave.Policy{}, fmt.Errorf("invalid policy %s: %w", path, validateErr)
	}
	return policy, nil
}

func removeOwnedSocket(path string, created os.FileInfo) {
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(created, current) {
		return
	}
	_ = os.Remove(path)
}
