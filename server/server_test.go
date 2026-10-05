package server_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/m1chlcz/agentwave"
	"github.com/m1chlcz/agentwave/server"
)

const actor = "11111111-1111-4111-8111-111111111111"

func testPolicy() agentwave.Policy {
	return agentwave.Policy{
		Actions:      []string{"help", "navigate", "lookup"},
		Modules:      []string{"none", "home", "items", "search"},
		RoutePrefix:  "/app",
		QueryActions: map[string]string{"lookup": "items"},
	}
}

func newTestStore(t *testing.T, generate agentwave.Generator) *agentwave.Store {
	t.Helper()
	store, err := agentwave.NewStore(context.Background(), testPolicy(), generate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	//nolint:usetesting // Unix socket paths must stay below the platform sockaddr_un limit, so t.TempDir is too long.
	dir, err := os.MkdirTemp("/tmp", "agentwave-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func serveRequest(
	handler http.Handler,
	method, path, body, identity string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if identity != "" {
		request.Header.Set(server.ActorHeader, identity)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertActorIdentitiesRejected(t *testing.T, handler http.Handler) {
	t.Helper()
	for _, identity := range []string{
		"",
		"not-a-uuid",
		strings.ToUpper("abcdefab-1111-4111-8111-111111111111"),
		"00000000-0000-0000-0000-000000000000",
		actor + "," + actor,
	} {
		if got := serveRequest(handler, "GET", "/job", "", identity); got.Code != 401 {
			t.Fatalf("identity %q: %d", identity, got.Code)
		}
	}
}

func assertObsoleteRoutesRejected(t *testing.T, handler http.Handler) {
	t.Helper()
	for _, path := range []string{"/jobs/123/apply", "/jobs/123/discard", "/jobs/123", "/job/"} {
		if got := serveRequest(handler, "POST", path, `{}`, actor); got.Code != 404 {
			t.Fatalf("obsolete route %s: %d", path, got.Code)
		}
	}
}

func assertInvalidBodiesRejected(t *testing.T, handler http.Handler) {
	t.Helper()
	for _, body := range []string{`{"prompt":"Help","route":"/app","command":"id"}`, `{}`, strings.Repeat("x", 17000)} {
		if got := serveRequest(handler, "POST", "/jobs", body, actor); got.Code != 422 {
			t.Fatalf("bad body: %d", got.Code)
		}
	}
}

func TestUnixAPIRoutesRequireActorAndScopeCurrentJob(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, func(ctx context.Context, _, _ string) (agentwave.Suggestion, error) {
		<-ctx.Done()
		return agentwave.Suggestion{}, ctx.Err()
	})
	handler := server.New(store)
	health := serveRequest(handler, "GET", "/healthz", "", "")
	if health.Code != 200 || health.Header().Get("Cache-Control") != "no-store" ||
		health.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("health: %#v", health)
	}
	assertActorIdentitiesRejected(t, handler)
	created := serveRequest(handler, "POST", "/jobs", `{"prompt":"Help","route":"/app"}`, actor)
	if created.Code != 202 {
		t.Fatalf("create: %d %s", created.Code, created.Body)
	}
	var fields map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 8 {
		t.Fatalf("unexpected response fields: %#v", fields)
	}
	other := "22222222-2222-4222-8222-222222222222"
	if got := serveRequest(handler, "GET", "/job", "", other); got.Code != 204 {
		t.Fatal("actor isolation", got.Code)
	}
	if got := serveRequest(handler, "GET", "/job?actor=other", "", actor); got.Code != 422 {
		t.Fatal("query accepted", got.Code)
	}
	if got := serveRequest(handler, "POST", "/jobs", `{"prompt":"Other","route":"/app"}`, actor); got.Code != 409 {
		t.Fatal("busy", got.Code)
	}
	if got := serveRequest(handler, "PUT", "/jobs", `{"prompt":"Other","route":"/app"}`, actor); got.Code != 405 ||
		got.Header().Get("Allow") != "POST" {
		t.Fatalf("method not allowed: %#v", got)
	}
	if got := serveRequest(
		handler,
		"POST",
		"/job",
		`{}`,
		actor,
	); got.Code != 405 ||
		got.Header().Get("Allow") != "GET" {
		t.Fatalf("method not allowed: %#v", got)
	}
	assertObsoleteRoutesRejected(t, handler)
	assertInvalidBodiesRejected(t, handler)
	oversized := strings.Repeat("x", 64*1024+1)
	if got := serveRequest(handler, "POST", "/jobs", oversized, actor); got.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body was not rejected before parsing: %d", got.Code)
	}
}

func TestContentTypeAndCustomActorHeader(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, nil)
	handler := server.NewWithActorHeader(store, "X-Custom-Actor")
	request := func(actorHeader, identity string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/job", nil)
		req.Header.Set(actorHeader, identity)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	if got := request(server.ActorHeader, actor); got.Code != 401 {
		t.Fatalf("default header accepted: %d", got.Code)
	}
	if got := request("X-Custom-Actor", actor); got.Code != 204 {
		t.Fatalf("custom header: %d", got.Code)
	}
	if got := server.NewWithActorHeader(store, ""); got == nil {
		t.Fatal("empty actor header must fall back")
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"prompt":"Help","route":"/app"}`))
	req.Header.Set(server.ActorHeader, actor)
	response := httptest.NewRecorder()
	server.New(store).ServeHTTP(response, req)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("missing content type: %d", response.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"prompt":"Help","route":"/app"}`))
	req.Header.Set(server.ActorHeader, actor)
	req.Header.Set("Content-Type", "text/plain")
	response = httptest.NewRecorder()
	server.New(store).ServeHTTP(response, req)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong content type: %d", response.Code)
	}
}

func TestListenUnixRefusesRegularFilesSymlinksAndLiveSockets(t *testing.T) {
	t.Parallel()

	path := filepath.Join(shortTempDir(t), "suggestion.sock")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := server.ListenUnix(path); err == nil {
		t.Fatal("replaced regular file")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", path); err != nil {
		t.Fatal(err)
	}
	if _, err := server.ListenUnix(path); err == nil {
		t.Fatal("replaced symlink")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	listener, err := server.ListenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0660 {
		t.Fatalf("socket mode: %v %v", info, err)
	}
	if _, listenErr := server.ListenUnix(path); listenErr == nil {
		t.Fatal("replaced live service socket")
	}
	if _, listenErr := server.ListenUnix("relative.sock"); listenErr == nil {
		t.Fatal("accepted relative socket path")
	}
}

func TestDuplicateActorHeaderFailsClosed(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/job", nil)
	req.Header.Add(server.ActorHeader, actor)
	req.Header.Add(server.ActorHeader, actor)
	response := httptest.NewRecorder()
	server.New(store).ServeHTTP(response, req)
	if response.Code != 401 || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response: %#v", response)
	}
}

func TestListenUnixReplacesOnlyAnOwnedStaleSocket(t *testing.T) {
	t.Parallel()

	path := filepath.Join(shortTempDir(t), "stale.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err = stale.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := server.ListenUnix(path)
	if err != nil {
		t.Fatalf("replace owned stale socket: %v", err)
	}
	defer listener.Close()
}
