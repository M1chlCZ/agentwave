package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/m1chlcz/agentwave"
	"github.com/m1chlcz/agentwave/server"
)

func echoGenerator(_ context.Context, prompt, _ string) (agentwave.Suggestion, error) {
	return agentwave.Suggestion{Message: prompt, Action: "help", Module: "none"}, nil
}

func submitPrompt(t *testing.T, store *agentwave.Store, handler http.Handler, prompt string) {
	t.Helper()
	body := `{"prompt":"` + prompt + `","route":"/app"}`
	if got := serveRequest(handler, http.MethodPost, "/jobs", body, actor); got.Code != http.StatusAccepted {
		t.Fatalf("submit %q: %d %s", prompt, got.Code, got.Body)
	}
	awaitReadyJob(t, store, actor)
}

func TestJobHistoryReturnsActorJobsMostRecentFirst(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, echoGenerator)
	handler := server.New(store)
	for _, prompt := range []string{"first", "second"} {
		submitPrompt(t, store, handler, prompt)
	}
	response := serveRequest(handler, http.MethodGet, "/jobs/history", "", actor)
	if response.Code != http.StatusOK {
		t.Fatalf("history status: %d %s", response.Code, response.Body)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("history content type: %q", contentType)
	}
	if cacheControl := response.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("history cache control: %q", cacheControl)
	}
	var entries []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0]["prompt"] != "second" || entries[1]["prompt"] != "first" {
		t.Fatalf("history order: %#v", entries)
	}
	for _, entry := range entries {
		if len(entry) != 9 || entry["status"] != "ready" || entry["finishedAt"] == nil || entry["id"] == nil {
			t.Fatalf("history entry: %#v", entry)
		}
		if _, leaked := entry["actor"]; leaked {
			t.Fatalf("actor leaked: %#v", entry)
		}
	}
	limited := serveRequest(handler, http.MethodGet, "/jobs/history?limit=1", "", actor)
	var limitedEntries []map[string]any
	if err := json.Unmarshal(limited.Body.Bytes(), &limitedEntries); err != nil {
		t.Fatal(err)
	}
	if limited.Code != http.StatusOK || len(limitedEntries) != 1 || limitedEntries[0]["prompt"] != "second" {
		t.Fatalf("limited history: %#v", limitedEntries)
	}
	if full := serveRequest(handler, http.MethodGet, "/jobs/history?limit=100", "", actor); full.Code != http.StatusOK {
		t.Fatalf("maximum limit: %d", full.Code)
	}
}

func TestJobHistoryEmptyIsJSONArray(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, echoGenerator)
	handler := server.New(store)
	response := serveRequest(handler, http.MethodGet, "/jobs/history", "", actor)
	if response.Code != http.StatusOK || response.Body.String() != "[]\n" {
		t.Fatalf("empty history: %d %q", response.Code, response.Body.String())
	}
	other := "22222222-2222-4222-8222-222222222222"
	response = serveRequest(handler, http.MethodGet, "/jobs/history", "", other)
	if response.Body.String() != "[]\n" {
		t.Fatalf("other actor history: %q", response.Body.String())
	}
	submitPrompt(t, store, handler, "first")
	response = serveRequest(handler, http.MethodGet, "/jobs/history", "", other)
	if response.Body.String() != "[]\n" {
		t.Fatalf("isolated history: %q", response.Body.String())
	}
}

func TestJobHistoryValidatesActorMethodAndLimit(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, nil)
	handler := server.New(store)
	for _, path := range []string{
		"/jobs/history?limit=0",
		"/jobs/history?limit=101",
		"/jobs/history?limit=abc",
		"/jobs/history?limit=1&limit=2",
		"/jobs/history?limit=-1",
		"/jobs/history?other=1",
		"/jobs/history?",
	} {
		if got := serveRequest(handler, http.MethodGet, path, "", actor); got.Code != http.StatusUnprocessableEntity {
			t.Fatalf("bad query %q: %d", path, got.Code)
		}
	}
	if got := serveRequest(handler, http.MethodPost, "/jobs/history", "", actor); got.Code != http.StatusMethodNotAllowed ||
		got.Header().Get("Allow") != "GET" {
		t.Fatalf("method: %#v", got)
	}
	if got := serveRequest(handler, http.MethodGet, "/jobs/history", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("missing actor: %d", got.Code)
	}
	if got := serveRequest(handler, http.MethodGet, "/jobs/history", "", "not-a-uuid"); got.Code != http.StatusUnauthorized {
		t.Fatalf("bad actor: %d", got.Code)
	}
}

func TestJobHistoryCapacityEviction(t *testing.T) {
	t.Parallel()

	store, err := agentwave.NewStoreWithOptions(
		context.Background(),
		testPolicy(),
		echoGenerator,
		agentwave.Options{HistoryCapacity: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	handler := server.New(store)
	for _, prompt := range []string{"one", "two", "three"} {
		submitPrompt(t, store, handler, prompt)
	}
	response := serveRequest(handler, http.MethodGet, "/jobs/history", "", actor)
	var entries []map[string]any
	if err = json.Unmarshal(response.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0]["prompt"] != "three" || entries[1]["prompt"] != "two" {
		t.Fatalf("eviction: %#v", entries)
	}
}

func TestJobHistoryConcurrentReaders(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, echoGenerator)
	handler := server.New(store)
	submitPrompt(t, store, handler, "first")
	var group sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 25; index++ {
				if got := serveRequest(handler, http.MethodGet, "/jobs/history?limit=5", "", actor); got.Code != http.StatusOK {
					t.Errorf("history status: %d", got.Code)
				}
				if got := serveRequest(handler, http.MethodGet, "/job", "", actor); got.Code != http.StatusOK {
					t.Errorf("job status: %d", got.Code)
				}
			}
		}()
	}
	group.Wait()
}

func TestOperationalEndpointsUnchangedAfterHistoryUse(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, echoGenerator)
	handler := server.NewWithOptions(store, server.Options{Version: "0.2.0"})
	submitPrompt(t, store, handler, "first")
	_ = serveRequest(handler, http.MethodGet, "/jobs/history", "", actor)
	health := serveRequest(handler, http.MethodGet, "/healthz", "", "")
	if health.Code != http.StatusOK || health.Body.String() != "ok\n" {
		t.Fatalf("health: %d %q", health.Code, health.Body.String())
	}
	metrics := serveRequest(handler, http.MethodGet, "/metrics", "", "")
	if metrics.Code != http.StatusOK {
		t.Fatalf("metrics status: %d", metrics.Code)
	}
	for _, want := range []string{
		`agentwave_build_info{version="0.2.0"} 1`,
		"agentwave_jobs_started_total 1",
		"agentwave_jobs_completed_total 1",
		"agentwave_jobs_failed_total 0",
		"agentwave_jobs_in_flight 0",
		"agentwave_entries_live 1",
	} {
		if !strings.Contains(metrics.Body.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.Body.String())
		}
	}
	current := serveRequest(handler, http.MethodGet, "/job", "", actor)
	var fields map[string]any
	if err := json.Unmarshal(current.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if current.Code != http.StatusOK || len(fields) != 8 {
		t.Fatalf("current job: %d %#v", current.Code, fields)
	}
}
