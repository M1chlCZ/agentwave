package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func writePolicy(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadPolicyAcceptsValidDocument(t *testing.T) {
	t.Parallel()

	path := writePolicy(t, `{
		"actions": ["help", "navigate", "lookup"],
		"modules": ["none", "home", "contacts", "deals", "reports"],
		"routePrefix": "/admin",
		"queryActions": {"lookup": "contacts"},
		"working": "Preparing a suggestion.",
		"failed": "The suggestion could not be prepared. Try again."
	}`)
	policy, err := loadPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.Actions) != 3 || !slices.Contains(policy.Modules, "contacts") || policy.RoutePrefix != "/admin" {
		t.Fatalf("policy: %#v", policy)
	}
	if policy.QueryActions["lookup"] != "contacts" || policy.Working == "" || policy.Failed == "" {
		t.Fatalf("policy fields: %#v", policy)
	}
	if _, routeErr := policy.ValidateInput("Where is the contact Acme?", "/admin/contacts"); routeErr != nil {
		t.Fatalf("in-scope route: %v", routeErr)
	}
}

func TestLoadPolicyRejectsUnknownField(t *testing.T) {
	t.Parallel()

	for _, content := range []string{
		`{"actions":["help"],"modules":["none"],"model":"other"}`,
		`{"actions":["help"],"modules":["none"],"route_prefix":"/admin"}`,
	} {
		if _, err := loadPolicy(writePolicy(t, content)); err == nil {
			t.Fatalf("unknown field accepted: %s", content)
		}
	}
}

func TestLoadPolicyRejectsInvalidPolicy(t *testing.T) {
	t.Parallel()

	for _, content := range []string{
		`{"actions":["navigate"],"modules":["none"]}`,
		`{"actions":["help"],"modules":["home"]}`,
		`{"actions":["help"],"modules":["none"],"queryActions":{"lookup":"none"}}`,
		`{"actions":["help"],"modules":["none"],"routePrefix":"admin"}`,
	} {
		if _, err := loadPolicy(writePolicy(t, content)); err == nil {
			t.Fatalf("invalid policy accepted: %s", content)
		}
	}
}

func TestLoadPolicyRejectsMissingAndUnreadableFile(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing.json")
	if _, err := loadPolicy(missing); err == nil || !strings.Contains(err.Error(), missing) {
		t.Fatalf("missing file: %v", err)
	}
	directory := t.TempDir()
	if _, err := loadPolicy(directory); err == nil {
		t.Fatal("directory accepted as policy")
	}
}

func TestPolicyExampleFileIsValid(t *testing.T) {
	t.Parallel()

	policy, err := loadPolicy("policy.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if policy.RoutePrefix != "/admin" || policy.QueryActions["lookup"] != "contacts" {
		t.Fatalf("example policy: %#v", policy)
	}
}

func TestFlagDefaults(t *testing.T) {
	t.Parallel()

	parsed, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.codex != "codex" || parsed.home != "" || parsed.version {
		t.Fatalf("defaults: %#v", parsed)
	}
	if parsed.socket != "" || parsed.policy != "" || parsed.model != "" {
		t.Fatalf("required flags have defaults: %#v", parsed)
	}
	custom, err := parseFlags([]string{"--codex", "/opt/codex", "--home", "/home/agent"}, io.Discard)
	if err != nil || custom.codex != "/opt/codex" || custom.home != "/home/agent" {
		t.Fatalf("custom flags: %#v %v", custom, err)
	}
}

func TestParseFlagsRejectsUnknownFlagAndArgument(t *testing.T) {
	t.Parallel()

	if _, err := parseFlags([]string{"--unknown"}, io.Discard); err == nil {
		t.Fatal("unknown flag accepted")
	}
	if _, err := parseFlags([]string{"extra"}, io.Discard); err == nil {
		t.Fatal("unexpected argument accepted")
	}
}

func TestVersionPrintsAndExitsZero(t *testing.T) {
	t.Parallel()

	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(version) {
		t.Fatalf("version %q is not a release version", version)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code %d: %s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != version {
		t.Fatalf("output %q", stdout.String())
	}
}

func TestRunRejectsMissingOrRelativeFlags(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{},
		{"--policy", "policy.json", "--model", "model"},
		{"--socket", "/tmp/agentwave-test.sock", "--model", "model"},
		{"--socket", "/tmp/agentwave-test.sock", "--policy", "policy.json"},
		{"--socket", "relative.sock", "--policy", "policy.json", "--model", "model"},
	} {
		var stderr bytes.Buffer
		if code := run(args, io.Discard, &stderr); code != 2 {
			t.Fatalf("args %v: exit code %d", args, code)
		}
		if stderr.Len() == 0 {
			t.Fatalf("args %v: no message", args)
		}
	}
}

func TestRunReportsConfigErrorsAndExitsTwo(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing.json")
	var stderr bytes.Buffer
	if code := run(
		[]string{"--socket", "/tmp/agentwave-test.sock", "--policy", missing, "--model", "model"},
		io.Discard,
		&stderr,
	); code != 2 {
		t.Fatalf("missing policy: exit code %d", code)
	}
	if !strings.Contains(stderr.String(), missing) {
		t.Fatalf("missing policy message: %q", stderr.String())
	}
	invalid := writePolicy(t, `{"actions":["navigate"],"modules":["none"]}`)
	stderr.Reset()
	if code := run(
		[]string{"--socket", "/tmp/agentwave-test.sock", "--policy", invalid, "--model", "model"},
		io.Discard,
		&stderr,
	); code != 2 {
		t.Fatalf("invalid policy: exit code %d", code)
	}
	if !strings.Contains(stderr.String(), "invalid policy") {
		t.Fatalf("invalid policy message: %q", stderr.String())
	}
}
