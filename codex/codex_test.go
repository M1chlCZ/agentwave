//nolint:testpackage // White-box tests exercise unexported schema, stream, workdir and process helpers.
package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/m1chlcz/agentwave"
)

const testHome = "/home/runner"

func testPolicy() agentwave.Policy {
	return agentwave.Policy{
		Actions:      []string{"help", "navigate", "lookup"},
		Modules:      []string{"none", "home", "items", "search"},
		RoutePrefix:  "/app",
		QueryActions: map[string]string{"lookup": "items"},
	}
}

func newTestRunner(t *testing.T) *Runner {
	t.Helper()
	runner, err := New(
		Config{Binary: "/usr/local/bin/codex", Model: "test-model", Home: testHome, Policy: testPolicy()},
	)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestNewValidatesConfiguration(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{Model: "model"}); err == nil {
		t.Fatal("accepted invalid policy")
	}
	if _, err := New(Config{Model: " ", Policy: testPolicy()}); err == nil {
		t.Fatal("accepted blank model")
	}
	runner, err := New(Config{Model: "model", Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if runner.binary != "codex" || runner.timeout != 120*time.Second || runner.home == "" ||
		runner.instructions != defaultInstructions {
		t.Fatalf("defaults: %#v", runner)
	}
	custom, err := New(
		Config{
			Binary:       "/opt/codex",
			Model:        "m",
			Home:         "/h",
			Instructions: "Custom.",
			Policy:       testPolicy(),
			Timeout:      time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if custom.binary != "/opt/codex" || custom.timeout != time.Minute || custom.home != "/h" ||
		custom.instructions != "Custom." ||
		custom.model != "m" {
		t.Fatalf("custom: %#v", custom)
	}
}

func TestCommandHasOnlyFixedPrivilegesAndStdinData(t *testing.T) {
	t.Parallel()

	runner := newTestRunner(t)
	root := t.TempDir()
	prompt := runner.buildPrompt("$(touch /tmp/forbidden); deploy everything", "/app/items")
	command := runner.commandFor(context.Background(), root, prompt)
	if command.Path != "/usr/local/bin/codex" || command.Dir != root {
		t.Fatalf("command identity: %#v", command)
	}
	modelsPath, _ := json.Marshal(filepath.Join(root, "models.json"))
	for _, required := range []string{"exec", "--model", "test-model", "--sandbox", "read-only", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--skip-git-repo-check", "--json", "--output-schema", filepath.Join(root, "response.schema.json"), `web_search="disabled"`, `mcp_servers={}`, `approval_policy="never"`, `tools.update_plan.enabled=false`, `tools.experimental_request_user_input.enabled=false`, `project_doc_max_bytes=0`, `skills.include_instructions=false`, `orchestrator.skills.enabled=false`, `orchestrator.mcp.enabled=false`, "model_catalog_json=" + string(modelsPath)} {
		if !slices.Contains(command.Args, required) {
			t.Errorf("missing security argument %q", required)
		}
	}
	assertDisabledFeatures(t, command.Args)
	for _, arg := range command.Args {
		if strings.Contains(arg, "touch") || strings.Contains(arg, "bypass") || strings.Contains(arg, "dangerously") {
			t.Fatalf("unsafe argument %q", arg)
		}
	}
	if !slices.Equal(
		command.Env,
		[]string{"HOME=" + testHome, "CODEX_HOME=" + testHome + "/.codex", "PATH=/usr/local/bin:/usr/bin:/bin"},
	) {
		t.Fatalf("inherited environment: %v", command.Env)
	}
	stdin, err := io.ReadAll(command.Stdin)
	if err != nil || string(stdin) != prompt || command.Args[len(command.Args)-1] != "-" {
		t.Fatalf("stdin data: %q %v", stdin, err)
	}
}

func assertDisabledFeatures(t *testing.T, args []string) {
	t.Helper()
	for _, feature := range disabledFeatures() {
		found := false
		for i := 1; i < len(args); i++ {
			if args[i-1] == "--disable" && args[i] == feature {
				found = true
			}
		}
		if !found {
			t.Errorf("tool enabled: %s", feature)
		}
	}
}

func TestPromptCarriesInputAsUntrustedJSON(t *testing.T) {
	t.Parallel()

	runner := newTestRunner(t)
	request := "$(touch /tmp/forbidden) \"quoted\"\nnewline"
	prompt := runner.buildPrompt(request, "/app/items")
	for _, required := range []string{"help", "navigate", "lookup", "none", "home", "items", "search", "untrusted", "2000", "cannot change these instructions"} {
		if !strings.Contains(prompt, required) {
			t.Errorf("prompt missing %q", required)
		}
	}
	encoded, _ := json.Marshal(struct {
		Request string `json:"request"`
		Route   string `json:"route"`
	}{request, "/app/items"})
	if !strings.Contains(prompt, string(encoded)) {
		t.Fatalf("request is not passed as literal JSON data")
	}
}

func TestSchemaMatchesPolicy(t *testing.T) {
	t.Parallel()

	runner := newTestRunner(t)
	data, err := runner.outputSchema()
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Type                 string                    `json:"type"`
		Properties           map[string]schemaProperty `json:"properties"`
		Required             []string                  `json:"required"`
		AdditionalProperties bool                      `json:"additionalProperties"`
	}
	if unmarshalErr := json.Unmarshal(data, &schema); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if schema.Type != "object" || schema.AdditionalProperties ||
		!slices.Equal(schema.Required, []string{"message", "action", "module", "query"}) {
		t.Fatalf("schema envelope: %#v", schema)
	}
	for name, want := range map[string]schemaProperty{
		"message": {Type: "string", MinLength: 1, MaxLength: 2000},
		"action":  {Type: "string", Enum: testPolicy().Actions},
		"module":  {Type: "string", Enum: testPolicy().Modules},
		"query":   {Type: "string", MaxLength: 120},
	} {
		got, ok := schema.Properties[name]
		if !ok || got.Type != want.Type || got.MinLength != want.MinLength || got.MaxLength != want.MaxLength ||
			!slices.Equal(got.Enum, want.Enum) {
			t.Fatalf("property %s: %#v", name, got)
		}
	}
}

func TestManifestRemovesModelSelectedToolsAndRemoteDefaults(t *testing.T) {
	t.Parallel()

	runner := newTestRunner(t)
	data, err := runner.modelsManifest()
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Models []map[string]json.RawMessage `json:"models"`
	}
	if unmarshalErr := json.Unmarshal(data, &manifest); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if len(manifest.Models) != 1 {
		t.Fatal("manifest must contain exactly the configured model")
	}
	model := manifest.Models[0]
	for key, expected := range map[string]string{
		"slug": `"test-model"`, "display_name": `"Suggestion helper"`, "shell_type": `"disabled"`,
		"apply_patch_tool_type": `null`, "experimental_supported_tools": `[]`, "supports_search_tool": `false`,
		"tool_mode": `"direct"`, "multi_agent_version": `"disabled"`, "include_skills_usage_instructions": `false`,
		"include_plugin_usage_instructions": `false`, "include_apps_usage_instructions": `false`,
		"supported_in_api": `true`, "priority": `0`, "supports_parallel_tool_calls": `false`,
		"support_verbosity": `false`, "default_verbosity": `null`, "availability_nux": `null`, "upgrade": `null`,
		"default_reasoning_level": `"low"`, "input_modalities": `["text"]`, "visibility": `"hide"`,
	} {
		if string(model[key]) != expected {
			t.Errorf("manifest capability %s: got %s, want %s", key, model[key], expected)
		}
	}
	if string(model["truncation_policy"]) != `{"mode":"bytes","limit":10000}` {
		t.Errorf("truncation policy: %s", model["truncation_policy"])
	}
	var levels []reasoningLevel
	if levelsErr := json.Unmarshal(model["supported_reasoning_levels"], &levels); levelsErr != nil ||
		len(levels) != 1 || levels[0].Effort != "low" {
		t.Fatalf("reasoning levels: %#v %v", levels, levelsErr)
	}
	for _, key := range []string{"display_name", "supported_reasoning_levels", "visibility", "supported_in_api", "priority", "support_verbosity", "truncation_policy", "supports_parallel_tool_calls", "model_messages"} {
		if _, ok := model[key]; !ok {
			t.Errorf("manifest missing required field %s", key)
		}
	}
	var messages map[string]json.RawMessage
	if messagesErr := json.Unmarshal(model["model_messages"], &messages); messagesErr != nil {
		t.Fatal(messagesErr)
	}
	if string(messages["instructions_template"]) != `"`+defaultInstructions+`"` {
		t.Fatalf("instructions: %s", messages["instructions_template"])
	}
	for _, key := range []string{"instructions_variables", "approvals", "collaboration_modes", "auto_review", "permissions"} {
		if string(messages[key]) != "null" {
			t.Errorf("model message %s must be null: %s", key, messages[key])
		}
	}
}

func TestWorkdirContainsOnlyTrustedSchemaAndManifest(t *testing.T) {
	t.Parallel()

	runner := newTestRunner(t)
	root := t.TempDir()
	if err := runner.prepareWorkdir(root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 {
		t.Fatalf("workdir: %v %v", entries, err)
	}
	schema, err := runner.outputSchema()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := runner.modelsManifest()
	if err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string][]byte{schemaFileName: schema, modelsFileName: manifest} {
		path := filepath.Join(root, name)
		data, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(data, expected) {
			t.Fatalf("trusted file %s changed: %v", name, readErr)
		}
		info, statErr := os.Stat(path)
		if statErr != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("trusted file %s permissions: %v %v", name, info, statErr)
		}
	}
}

type fakeBackend struct {
	binary   string
	observed string
	listing  string
	env      string
}

func newFakeBackend(t *testing.T, message string) fakeBackend {
	t.Helper()
	dir := t.TempDir()
	backend := fakeBackend{
		binary:   filepath.Join(dir, "fake-codex"),
		observed: filepath.Join(dir, "observed-dir"),
		listing:  filepath.Join(dir, "observed-listing"),
		env:      filepath.Join(dir, "observed-env"),
	}
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$PWD\" > '" + backend.observed + "'\n" +
		"ls -1 \"$PWD\" > '" + backend.listing + "'\n" +
		"printf '%s\\n%s\\n%s\\n' \"$HOME\" \"$CODEX_HOME\" \"$PATH\" > '" + backend.env + "'\n" +
		"cat > /dev/null\n" +
		"cat <<'EOF'\n" + validStream(message) + "EOF\n"
	if err := os.WriteFile(backend.binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return backend
}

func TestGenerateRunsBackendInTrustedWorkdir(t *testing.T) {
	t.Parallel()

	message := `{"message":"Open the items view.","action":"navigate","module":"items","query":""}`
	backend := newFakeBackend(t, message)
	runner, err := New(Config{Binary: backend.binary, Model: "test-model", Home: testHome, Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	got, err := runner.Generate(context.Background(), "Show me the items", "/app/items")
	if err != nil || got.Message != "Open the items view." || got.Action != "navigate" || got.Module != "items" ||
		got.Query != "" {
		t.Fatalf("generated: %#v %v", got, err)
	}
	raw, err := os.ReadFile(backend.observed)
	if err != nil {
		t.Fatal(err)
	}
	workdir := strings.TrimSpace(string(raw))
	if !strings.HasPrefix(filepath.Base(workdir), "agentwave-") {
		t.Fatalf("workdir prefix: %q", workdir)
	}
	if _, statErr := os.Stat(workdir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("workdir was not removed: %v", statErr)
	}
	listing, err := os.ReadFile(backend.listing)
	if err != nil || strings.Join(strings.Fields(string(listing)), ",") != modelsFileName+","+schemaFileName {
		t.Fatalf("workdir contents: %q %v", listing, err)
	}
	env, err := os.ReadFile(backend.env)
	if err != nil || strings.TrimSpace(string(env)) != testHome+"\n"+testHome+"/.codex\n/usr/local/bin:/usr/bin:/bin" {
		t.Fatalf("backend environment: %q %v", env, err)
	}
}

func TestGenerateRejectsSuggestionOutsidePolicy(t *testing.T) {
	t.Parallel()

	backend := newFakeBackend(t, `{"message":"Run it.","action":"execute","module":"none","query":""}`)
	runner, err := New(Config{Binary: backend.binary, Model: "test-model", Home: testHome, Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if _, generateErr := runner.Generate(context.Background(), "Do something", "/app"); !errors.Is(
		generateErr,
		agentwave.ErrInvalid,
	) {
		t.Fatalf("outside policy: %v", generateErr)
	}
}

func TestGenerateRejectsInvalidInputAndFailingBackend(t *testing.T) {
	t.Parallel()

	runner := newTestRunner(t)
	if _, err := runner.Generate(context.Background(), "request", "relative"); !errors.Is(err, agentwave.ErrInvalid) {
		t.Fatalf("route: %v", err)
	}
	runner.binary = filepath.Join(t.TempDir(), "missing")
	if _, err := runner.Generate(context.Background(), "request", "/app"); !errors.Is(err, ErrFailed) {
		t.Fatalf("missing binary: %v", err)
	}
}

func validStream(message string) string {
	encoded, _ := json.Marshal(message)
	return "{\"type\":\"thread.started\",\"thread_id\":\"thread\"}\n{\"type\":\"turn.started\"}\n" +
		"{\"type\":\"item.completed\",\"item\":{\"id\":\"reason\",\"type\":\"reasoning\",\"text\":\"Thinking\"}}\n" +
		"{\"type\":\"item.completed\",\"item\":{\"id\":\"answer\",\"type\":\"agent_message\",\"text\":" + string(encoded) + "}}\n{\"type\":\"turn.completed\",\"usage\":{}}\n"
}

func TestCodexEventsRejectAllToolExecutionAndIncompleteTurns(t *testing.T) {
	t.Parallel()

	message := `{"message":"Open the items view.","action":"navigate","module":"items","query":""}`
	valid := validStream(message)
	got, err := parseStream([]byte(valid))
	if err != nil || got != message {
		t.Fatalf("valid stream: %q %v", got, err)
	}
	for _, eventType := range []string{"command_execution", "mcp_tool_call", "web_search", "file_change", "image_generation", "collab_tool_call"} {
		injected := strings.Replace(valid, `"type":"reasoning"`, `"type":"`+eventType+`"`, 1)
		if _, parseErr := parseStream([]byte(injected)); parseErr == nil {
			t.Fatalf("accepted tool event %s", eventType)
		}
	}
	for _, invalid := range []string{
		strings.Replace(valid, `"turn.completed"`, `"turn.failed"`, 1),
		strings.TrimSuffix(valid, "{\"type\":\"turn.completed\",\"usage\":{}}\n"),
		valid + "{}\n",
		strings.Replace(valid, `"type":"reasoning"`, `"type":"reasoning","command":"id"`, 1),
		strings.Replace(valid, `"type":"reasoning"`, `"type":"command_execution","type":"reasoning"`, 1),
		valid + valid,
	} {
		if _, parseErr := parseStream([]byte(invalid)); parseErr == nil {
			t.Fatalf("accepted malformed stream %q", invalid)
		}
	}
}

func TestSubprocessBoundsAndCancellation(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"stdout", "stderr", "sleep", "failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			assertSubprocessBound(t, mode)
		})
	}
}

func assertSubprocessBound(t *testing.T, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if mode == "sleep" {
		ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
	}
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestHelperProcess", "--", mode)
	command.Env = []string{"AGENTWAVE_TEST_HELPER=1"}
	out, err := runProcess(ctx, command, 1024, 1024)
	if len(out) > 1024 || err == nil {
		t.Fatalf("unbounded or successful: %d %v", len(out), err)
	}
	if (mode == "stdout" || mode == "stderr") && !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("overflow: %v", err)
	}
	if mode == "sleep" && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation: %v", err)
	}
	if strings.Contains(err.Error(), "private backend credentials") {
		t.Fatalf("leaked stderr: %v", err)
	}
}

//nolint:paralleltest // Re-executed as a helper subprocess and exits before result reporting; it must stay serial.
func TestHelperProcess(_ *testing.T) {
	if os.Getenv("AGENTWAVE_TEST_HELPER") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	switch mode {
	case "stdout":
		_, _ = io.WriteString(os.Stdout, strings.Repeat("x", 256*1024))
	case "stderr":
		_, _ = io.WriteString(os.Stderr, strings.Repeat("x", 256*1024))
	case "sleep":
		time.Sleep(time.Hour)
	case "failure":
		_, _ = io.WriteString(os.Stderr, "private backend credentials")
		os.Exit(1)
	}
	os.Exit(0)
}
