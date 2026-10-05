//nolint:testpackage // White-box tests exercise unexported status, action, module and limit constants plus Store internals.
package agentwave

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const actorA = "11111111-1111-4111-8111-111111111111"
const actorB = "22222222-2222-4222-8222-222222222222"
const actorC = "33333333-3333-4333-8333-333333333333"

func testPolicy() Policy {
	return Policy{
		Actions:      []string{"help", "navigate", "lookup"},
		Modules:      []string{"none", "home", "items", "search"},
		RoutePrefix:  "/app",
		QueryActions: map[string]string{"lookup": "items"},
	}
}

func TestPolicyValidation(t *testing.T) {
	t.Parallel()

	for _, policy := range []Policy{
		{Actions: []string{"help", "run"}, Modules: []string{"none"}},
		{Actions: []string{"help", "a1_b2"}, Modules: []string{"none"}},
		{Actions: []string{"help", "navigate", "lookup"}, Modules: []string{"none", "items"}, RoutePrefix: "/"},
		testPolicy(),
	} {
		if err := policy.Validate(); err != nil {
			t.Fatalf("valid policy rejected: %#v %v", policy, err)
		}
	}
	for _, policy := range []Policy{
		{},
		{Modules: []string{"none"}},
		{Actions: []string{"run"}, Modules: []string{"none"}},
		{Actions: []string{"help"}},
		{Actions: []string{""}, Modules: []string{"none"}},
		{Actions: []string{"Help"}, Modules: []string{"none"}},
		{Actions: []string{"1help"}, Modules: []string{"none"}},
		{Actions: []string{"help", "help"}, Modules: []string{"none"}},
		{Actions: []string{"help"}, Modules: []string{"none", "none"}},
		{Actions: []string{"help"}, Modules: []string{"home"}},
		{Actions: []string{"help"}, Modules: []string{"none"}, QueryActions: map[string]string{"lookup": "none"}},
		{Actions: []string{"help", "lookup"}, Modules: []string{"none"}, QueryActions: map[string]string{"lookup": "items"}},
		{Actions: []string{"help"}, Modules: []string{"none"}, RoutePrefix: "/app/"},
		{Actions: []string{"help"}, Modules: []string{"none"}, RoutePrefix: "app"},
		{Actions: []string{"help", "navigate"}, Modules: []string{"none"}, QueryActions: map[string]string{"help": "none"}},
		{Actions: []string{"help", "navigate"}, Modules: []string{"none"}, QueryActions: map[string]string{"navigate": "none"}},
	} {
		err := policy.Validate()
		if err == nil {
			t.Fatalf("invalid policy accepted: %#v", policy)
		}
		if !strings.Contains(err.Error(), "policy") {
			t.Fatalf("unhelpful policy error: %v", err)
		}
	}
}

func TestPolicyDefaultsAndRouteScope(t *testing.T) {
	t.Parallel()

	minimal := Policy{Actions: []string{"help"}, Modules: []string{"none"}}
	store, err := NewStore(context.Background(), minimal, func(context.Context, string, string) (Suggestion, error) {
		return Suggestion{}, errors.New("provider failure")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	created, err := store.Submit(actorA, "Any request", "/deep/link")
	if err != nil || created.Message != defaultWorking || created.Action != actionHelp || created.Module != moduleNone {
		t.Fatalf("working defaults: %#v %v", created, err)
	}
	finished := awaitFinished(t, store, actorA)
	if finished.Status != statusFailed || finished.Message != defaultFailed {
		t.Fatalf("failure defaults: %#v", finished)
	}
	if _, err = store.Submit(actorB, "Any request", "relative"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("relative route: %v", err)
	}
	for _, route := range []string{"/app", "/app/items"} {
		if _, err = testPolicy().ValidateInput("Request", route); err != nil {
			t.Fatalf("in-scope route %q: %v", route, err)
		}
	}
	for _, route := range []string{"/app/", "/application", "/other", "app", ""} {
		if _, err = testPolicy().ValidateInput("Request", route); !errors.Is(err, ErrInvalid) {
			t.Fatalf("out-of-scope route %q: %v", route, err)
		}
	}
	root, err := NewStore(context.Background(), minimal, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	if _, err = root.policy.ValidateInput("Request", "/anything"); err != nil {
		t.Fatalf("default prefix: %v", err)
	}
}

func TestActorIsolationAndBoundedGeneration(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(ctx context.Context, prompt, _ string) (Suggestion, error) {
			started <- struct{}{}
			select {
			case <-release:
				return Suggestion{Message: prompt, Action: "lookup", Module: "items", Query: "hammer"}, nil
			case <-ctx.Done():
				return Suggestion{}, ctx.Err()
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	first, err := store.Submit(actorA, "First request", "/app/items")
	if err != nil || first.Status != statusWorking || len(first.ID) != 32 || first.Action != actionHelp ||
		first.Module != moduleNone {
		t.Fatalf("submit = %#v, %v", first, err)
	}
	<-started
	if _, exists := store.Current(actorB); exists {
		t.Fatal("another actor saw the job")
	}
	if _, err = store.Submit(actorA, "Second", "/app"); !errors.Is(err, ErrBusy) {
		t.Fatalf("same actor: %v", err)
	}
	if _, err = store.Submit(actorB, "Second", "/app"); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err = store.Submit(actorC, "Third", "/app"); !errors.Is(err, ErrBusy) {
		t.Fatalf("global limit: %v", err)
	}
	close(release)
	got := awaitFinished(t, store, actorA)
	if got.ID != first.ID || got.Status != statusReady || got.Message != "First request" {
		t.Fatalf("finished: %#v", got)
	}
}

func TestEntryCapacityAndExpiry(t *testing.T) {
	t.Parallel()

	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(context.Context, string, string) (Suggestion, error) {
			return Suggestion{Message: "Open the items view.", Action: "navigate", Module: "items"}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	now := time.Now()
	store.now = func() time.Time { return now }
	for i := 1; i <= entryLimit; i++ {
		actor := fmt.Sprintf("%08x-1111-4111-8111-111111111111", i)
		if _, submitErr := store.Submit(actor, "Request", "/app"); submitErr != nil {
			t.Fatal(submitErr)
		}
		awaitFinished(t, store, actor)
	}
	if _, capacityErr := store.Submit(actorA, "Request", "/app"); !errors.Is(capacityErr, ErrCapacity) {
		t.Fatalf("capacity: %v", capacityErr)
	}
	now = now.Add(entryTTL)
	if _, ok := store.Current("00000001-1111-4111-8111-111111111111"); ok {
		t.Fatal("expired job remains visible")
	}
	if _, retryErr := store.Submit(actorA, "Request", "/app"); retryErr != nil {
		t.Fatalf("expired capacity: %v", retryErr)
	}
}

func TestCancellationAndFailureDoNotExposeProviderDetails(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"shutdown", "deadline", "invalid", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			started := make(chan struct{})
			store, err := NewStore(
				context.Background(),
				testPolicy(),
				func(ctx context.Context, _, _ string) (Suggestion, error) {
					close(started)
					switch mode {
					case "invalid":
						return Suggestion{Message: "private", Action: "execute", Module: "none"}, nil
					case "error":
						return Suggestion{}, errors.New("secret provider details")
					case "panic":
						panic("secret provider details")
					default:
						<-ctx.Done()
						return Suggestion{}, ctx.Err()
					}
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			store.timeout = 20 * time.Millisecond
			t.Cleanup(store.Close)
			if _, err = store.Submit(actorA, "Help", "/app"); err != nil {
				t.Fatal(err)
			}
			<-started
			if mode == "shutdown" {
				store.Close()
			}
			got := awaitFinished(t, store, actorA)
			if got.Status != statusFailed || got.Action != actionHelp || got.Module != moduleNone || got.Query != "" ||
				got.Message != defaultFailed || strings.Contains(got.Message, "secret") || strings.Contains(got.Message, "private") {
				t.Fatalf("unsafe failure: %#v", got)
			}
		})
	}
}

func TestStrictInputAndSuggestionBoundary(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		`{"message":"OK","action":"help","module":"none"}`,
		`{"message":"OK","action":"help","module":"none","query":"","extra":"id"}`,
		`{"message":"OK","action":"help","module":"none","query":""} {}`,
		`{"message":"OK","action":"help","module":"none","query":"","action":"navigate"}`,
		`{"message":"OK","action":"navigate","module":"none","query":""}`,
		`{"message":"OK","action":"navigate","module":"items","query":"hammer"}`,
		`{"message":"OK","action":"lookup","module":"items","query":""}`,
		`{"message":"OK","action":"lookup","module":"search","query":"hammer"}`,
		`{"message":"OK","action":"lookup","module":"items","query":null}`,
		`{"message":"OK","action":"execute","module":"none","query":"id"}`,
		`{"message":"OK","action":"help","module":"https://evil.test","query":""}`,
		`{"message":"OK","action":"lookup","module":"items","query":"` + strings.Repeat("漢", 41) + `"}`,
		`{"message":"` + strings.Repeat("a", 2001) + `","action":"help","module":"none","query":""}`,
		`{"message":"OK","action":"help","module":"none","query":"` + "a\nb" + `"}`,
		`[{"message":"OK","action":"help","module":"none","query":""}]`,
	} {
		if got, err := testPolicy().ParseSuggestion([]byte(raw)); err == nil {
			t.Fatalf("accepted %s: %#v", raw, got)
		}
	}
	got, err := testPolicy().ParseSuggestion([]byte(`{"message":"Find the item.","action":"lookup","module":"items","query":"$(touch /tmp/forbidden); rm -rf /"}`))
	if err != nil || got.Action != "lookup" || got.Query != "$(touch /tmp/forbidden); rm -rf /" {
		t.Fatalf("query must stay literal data: %#v %v", got, err)
	}
	for _, route := range []string{"//evil.test", "/app/../api", "/app?token=private", "/app/%2e%2e", "/app\\foo", "/app/\n"} {
		if _, routeErr := testPolicy().ValidateInput("Help", route); routeErr == nil {
			t.Fatalf("accepted route %q", route)
		}
	}
	if _, overrideErr := testPolicy().ParseInput([]byte(`{"prompt":"Help","route":"/app","actor":"forged"}`)); overrideErr == nil {
		t.Fatal("accepted actor override")
	}
	if _, duplicateErr := testPolicy().ParseInput([]byte(`{"prompt":"Help","prompt":"Other","route":"/app"}`)); duplicateErr == nil {
		t.Fatal("accepted duplicate input")
	}
	if _, inputErr := testPolicy().ParseInput([]byte(strings.Repeat("x", inputLimit+1))); inputErr == nil {
		t.Fatal("accepted oversized input document")
	}
	if _, suggestionErr := testPolicy().ParseSuggestion([]byte(strings.Repeat("x", suggestionLimit+1))); suggestionErr == nil {
		t.Fatal("accepted oversized suggestion document")
	}
	large, err := json.Marshal(Input{Prompt: strings.Repeat("<", 2000), Route: "/app/" + strings.Repeat("&", 2000)})
	if err != nil || len(large) < 16*1024 {
		t.Fatalf("escaped fixture: %d %v", len(large), err)
	}
	if _, parseErr := testPolicy().ParseInput(large); parseErr != nil {
		t.Fatalf("rejected valid escaped input: %v", parseErr)
	}
}

func TestSuggestionValidationMatrix(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		suggestion Suggestion
		valid      bool
	}{
		{"help", Suggestion{Message: "Ask a question.", Action: "help", Module: "none"}, true},
		{"help with module", Suggestion{Message: "Ask a question.", Action: "help", Module: "search"}, true},
		{"help with query", Suggestion{Message: "Ask.", Action: "help", Module: "none", Query: "hammer"}, false},
		{"navigate", Suggestion{Message: "Open home.", Action: "navigate", Module: "home"}, true},
		{"navigate none", Suggestion{Message: "Open none.", Action: "navigate", Module: "none"}, false},
		{"navigate with query", Suggestion{Message: "Open home.", Action: "navigate", Module: "home", Query: "hammer"}, false},
		{"query action", Suggestion{Message: "Find it.", Action: "lookup", Module: "items", Query: "hammer"}, true},
		{"query action empty", Suggestion{Message: "Find it.", Action: "lookup", Module: "items"}, false},
		{"query action wrong module", Suggestion{Message: "Find it.", Action: "lookup", Module: "search", Query: "hammer"}, false},
		{"unlisted action", Suggestion{Message: "Run it.", Action: "execute", Module: "none"}, false},
		{"unlisted module", Suggestion{Message: "Open it.", Action: "navigate", Module: "elsewhere"}, false},
		{"blank message", Suggestion{Message: "   ", Action: "help", Module: "none"}, false},
		{"long message", Suggestion{Message: strings.Repeat("a", messageLimit+1), Action: "help", Module: "none"}, false},
		{"invalid message", Suggestion{Message: string([]byte{0xff, 0xfe}), Action: "help", Module: "none"}, false},
		{"padded query", Suggestion{Message: "Find it.", Action: "lookup", Module: "items", Query: " hammer "}, false},
		{"long query", Suggestion{Message: "Find it.", Action: "lookup", Module: "items", Query: strings.Repeat("a", queryLimit+1)}, false},
		{"control rune query", Suggestion{Message: "Find it.", Action: "lookup", Module: "items", Query: "ham\tmer"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := testPolicy().ValidateSuggestion(test.suggestion)
			if test.valid && err != nil {
				t.Fatalf("rejected valid suggestion: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatalf("accepted invalid suggestion: %#v", test.suggestion)
			}
		})
	}
}

func TestSubmitRejectsBadActorsAndInput(t *testing.T) {
	t.Parallel()

	store, err := NewStore(context.Background(), testPolicy(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	for _, actor := range []string{"", "not-a-uuid", strings.ToUpper("abcdefab-1111-4111-8111-111111111111"), "00000000-0000-0000-0000-000000000000", actorA + "," + actorA} {
		if _, submitErr := store.Submit(actor, "Request", "/app"); !errors.Is(submitErr, ErrInvalid) {
			t.Fatalf("actor %q: %v", actor, submitErr)
		}
		if _, ok := store.Current(actor); ok {
			t.Fatalf("actor %q has a job", actor)
		}
	}
	if _, blankErr := store.Submit(actorA, "   ", "/app"); !errors.Is(blankErr, ErrInvalid) {
		t.Fatalf("blank prompt: %v", blankErr)
	}
	if _, routeErr := store.Submit(actorA, "Request", "/outside"); !errors.Is(routeErr, ErrInvalid) {
		t.Fatalf("out-of-scope route: %v", routeErr)
	}
}

func TestNilGeneratorAndClosedStoreReportCapacity(t *testing.T) {
	t.Parallel()

	store, err := NewStore(context.Background(), testPolicy(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, nilGenErr := store.Submit(actorA, "Request", "/app"); !errors.Is(nilGenErr, ErrCapacity) {
		t.Fatalf("nil generator: %v", nilGenErr)
	}
	store.Close()
	store.Close()
	if _, closedErr := store.Submit(actorA, "Request", "/app"); !errors.Is(closedErr, ErrCapacity) {
		t.Fatalf("closed store: %v", closedErr)
	}
}

func awaitFinished(t *testing.T, store *Store, actor string) Job {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if current, ok := store.Current(actor); ok && current.Status != statusWorking {
			return current
		}
		select {
		case <-deadline:
			t.Fatal("generation did not finish")
		case <-tick.C:
		}
	}
}
