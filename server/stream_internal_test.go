package server

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m1chlcz/agentwave"
)

func TestJobEventsSendsKeepAliveComments(t *testing.T) {
	previous := sseKeepAlive
	sseKeepAlive = 20 * time.Millisecond
	t.Cleanup(func() { sseKeepAlive = previous })
	policy := agentwave.Policy{Actions: []string{"help"}, Modules: []string{"none"}}
	store, err := agentwave.NewStore(
		context.Background(),
		policy,
		func(ctx context.Context, _, _ string) (agentwave.Suggestion, error) {
			<-ctx.Done()
			return agentwave.Suggestion{}, ctx.Err()
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	const identity = "11111111-1111-4111-8111-111111111111"
	created, err := store.Submit(identity, "Help", "/")
	if err != nil {
		t.Fatal(err)
	}
	service := httptest.NewServer(New(store))
	t.Cleanup(service.Close)
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, service.URL+"/jobs/"+created.ID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(ActorHeader, identity)
	response, err := service.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatalf("read keep-alive: %v", readErr)
		}
		if strings.TrimSuffix(line, "\n") == ": keep-alive" {
			return
		}
	}
	t.Fatal("no keep-alive comment")
}
