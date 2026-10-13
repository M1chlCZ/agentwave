package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m1chlcz/agentwave"
	"github.com/m1chlcz/agentwave/server"
)

type streamEvent struct {
	name string
	data string
}

func gatedGenerator(release <-chan struct{}) agentwave.Generator {
	return func(ctx context.Context, prompt, _ string) (agentwave.Suggestion, error) {
		select {
		case <-release:
			return agentwave.Suggestion{Message: prompt, Action: "help", Module: "none"}, nil
		case <-ctx.Done():
			return agentwave.Suggestion{}, ctx.Err()
		}
	}
}

func readStreamEvent(t *testing.T, reader *bufio.Reader) (streamEvent, bool) {
	t.Helper()
	var event streamEvent
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && event.name == "" && event.data == "" {
				return streamEvent{}, false
			}
			t.Fatalf("read stream: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case strings.HasPrefix(line, ":"):
		case line == "":
			if event.name != "" || event.data != "" {
				return event, true
			}
		case strings.HasPrefix(line, "event: "):
			event.name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			event.data = strings.TrimPrefix(line, "data: ")
		default:
			t.Fatalf("unexpected stream line: %q", line)
		}
	}
}

func awaitWatcherCount(t *testing.T, store *agentwave.Store, want int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if store.Stats().Watchers == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("watchers never reached %d", want)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestJobEventsReplaysStateAndStreamsTransition(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	store := newTestStore(t, gatedGenerator(release))
	created, err := store.Submit(actor, "Where do I find the contact?", "/app/contacts")
	if err != nil {
		t.Fatal(err)
	}
	service := httptest.NewServer(server.New(store))
	t.Cleanup(service.Close)
	request, err := http.NewRequest(http.MethodGet, service.URL+"/jobs/"+created.ID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(server.ActorHeader, actor)
	response, err := service.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", response.StatusCode)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "text/event-stream" {
		t.Fatalf("content type: %q", contentType)
	}
	if cacheControl := response.Header.Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("cache control: %q", cacheControl)
	}
	reader := bufio.NewReader(response.Body)
	replay, ok := readStreamEvent(t, reader)
	if !ok || replay.name != "working" {
		t.Fatalf("replay: %#v %v", replay, ok)
	}
	var replayJob agentwave.Job
	if err = json.Unmarshal([]byte(replay.data), &replayJob); err != nil ||
		replayJob.ID != created.ID || replayJob.Status != "working" {
		t.Fatalf("replay data: %#v %v", replayJob, err)
	}
	close(release)
	update, ok := readStreamEvent(t, reader)
	if !ok || update.name != "ready" {
		t.Fatalf("update: %#v %v", update, ok)
	}
	var updateJob agentwave.Job
	if err = json.Unmarshal([]byte(update.data), &updateJob); err != nil || updateJob.Status != "ready" ||
		updateJob.Message != "Where do I find the contact?" {
		t.Fatalf("update data: %#v %v", updateJob, err)
	}
	if _, ok = readStreamEvent(t, reader); ok {
		t.Fatal("stream did not close after the terminal event")
	}
}

func TestJobEventsReplaysFinishedJobAndCloses(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, echoGenerator)
	created, err := store.Submit(actor, "Help", "/app")
	if err != nil {
		t.Fatal(err)
	}
	awaitReadyJob(t, store, actor)
	service := httptest.NewServer(server.New(store))
	t.Cleanup(service.Close)
	request, err := http.NewRequest(http.MethodGet, service.URL+"/jobs/"+created.ID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(server.ActorHeader, actor)
	response, err := service.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	reader := bufio.NewReader(response.Body)
	event, ok := readStreamEvent(t, reader)
	if !ok || event.name != "ready" {
		t.Fatalf("finished replay: %#v %v", event, ok)
	}
	if _, ok = readStreamEvent(t, reader); ok {
		t.Fatal("stream did not close")
	}
}

func TestJobEventsOnceReturnsStateAndCloses(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	store := newTestStore(t, gatedGenerator(release))
	created, err := store.Submit(actor, "Help", "/app")
	if err != nil {
		t.Fatal(err)
	}
	service := httptest.NewServer(server.New(store))
	t.Cleanup(service.Close)
	request, err := http.NewRequest(http.MethodGet, service.URL+"/jobs/"+created.ID+"/events?once=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(server.ActorHeader, actor)
	response, err := service.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	event, ok := readStreamEvent(t, reader)
	if !ok || event.name != "working" {
		t.Fatalf("once replay: %#v %v", event, ok)
	}
	if _, ok = readStreamEvent(t, reader); ok {
		t.Fatal("once stream did not close")
	}
	close(release)
}

func TestJobEventsRejectsBadRequests(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, nil)
	handler := server.New(store)
	id := strings.Repeat("ab", 16)
	for _, path := range []string{
		"/jobs/" + id + "/events?once=0",
		"/jobs/" + id + "/events?once=1&once=1",
		"/jobs/" + id + "/events?once=1&limit=1",
		"/jobs/" + id + "/events?other=1",
		"/jobs/" + id + "/events?",
	} {
		if got := serveRequest(handler, http.MethodGet, path, "", actor); got.Code != http.StatusUnprocessableEntity {
			t.Fatalf("bad query %q: %d", path, got.Code)
		}
	}
	for _, path := range []string{
		"/jobs/" + id + "/events",
		"/jobs/not-a-job/events",
		"/jobs/" + strings.Repeat("A", 32) + "/events",
		"/jobs/" + id + "/events/",
		"/jobs/history/events",
	} {
		if got := serveRequest(handler, http.MethodGet, path, "", actor); got.Code != http.StatusNotFound {
			t.Fatalf("missing job %q: %d", path, got.Code)
		}
	}
	if got := serveRequest(handler, http.MethodGet, "/jobs/"+id+"/events", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("missing actor: %d", got.Code)
	}
	if got := serveRequest(handler, http.MethodPost, "/jobs/"+id+"/events", "", actor); got.Code != http.StatusMethodNotAllowed ||
		got.Header().Get("Allow") != "GET" {
		t.Fatalf("method: %#v", got)
	}
}

func TestJobEventsClientDisconnectReleasesWatcher(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	store := newTestStore(t, gatedGenerator(release))
	created, err := store.Submit(actor, "Help", "/app")
	if err != nil {
		t.Fatal(err)
	}
	service := httptest.NewServer(server.New(store))
	t.Cleanup(service.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, service.URL+"/jobs/"+created.ID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(server.ActorHeader, actor)
	response, err := service.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	if _, ok := readStreamEvent(t, reader); !ok {
		t.Fatal("missing replay")
	}
	awaitWatcherCount(t, store, 1)
	cancel()
	awaitWatcherCount(t, store, 0)
	close(release)
}
