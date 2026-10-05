// Package server exposes the agent assistant suggestion service exclusively
// through a private Unix socket.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/m1chlcz/agentwave"
)

// ActorHeader is the default request header that identifies the calling actor.
const ActorHeader = "X-Actor-Id"

const (
	maxRequestBodyBytes = 64 * 1024
	socketProbeTimeout  = 200 * time.Millisecond
)

// New returns a handler that reads the actor from ActorHeader.
func New(store *agentwave.Store) http.Handler {
	return NewWithActorHeader(store, ActorHeader)
}

// NewWithActorHeader returns a handler that reads the actor from actorHeader;
// an empty actorHeader falls back to ActorHeader.
func NewWithActorHeader(store *agentwave.Store, actorHeader string) http.Handler {
	if actorHeader == "" {
		actorHeader = ActorHeader
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		serve(store, actorHeader, response, request)
	})
}

func serve(store *agentwave.Store, actorHeader string, response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if isHealthRequest(request) {
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(response, "ok\n")
		return
	}
	actor, valid := requestActor(request, actorHeader)
	if !valid {
		fail(response, http.StatusUnauthorized)
		return
	}
	if request.URL.RawQuery != "" || request.URL.ForceQuery || request.URL.RawPath != "" {
		fail(response, http.StatusUnprocessableEntity)
		return
	}
	switch request.URL.Path {
	case "/job":
		serveCurrentJob(store, actor, response, request)
	case "/jobs":
		serveSubmit(store, actor, response, request)
	default:
		fail(response, http.StatusNotFound)
	}
}

func isHealthRequest(request *http.Request) bool {
	return request.URL.Path == "/healthz" && request.Method == http.MethodGet && request.URL.RawQuery == ""
}

func requestActor(request *http.Request, actorHeader string) (string, bool) {
	actors := request.Header.Values(actorHeader)
	if len(actors) != 1 || !agentwave.ValidActor(actors[0]) {
		return "", false
	}
	return actors[0], true
}

func serveCurrentJob(store *agentwave.Store, actor string, response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", "GET")
		fail(response, http.StatusMethodNotAllowed)
		return
	}
	current, found := store.Current(actor)
	if !found {
		response.WriteHeader(http.StatusNoContent)
		return
	}
	writeJob(response, current, http.StatusOK)
}

func serveSubmit(store *agentwave.Store, actor string, response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", "POST")
		fail(response, http.StatusMethodNotAllowed)
		return
	}
	contentType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		fail(response, http.StatusUnsupportedMediaType)
		return
	}
	body, read := readSubmitBody(response, request)
	if !read {
		return
	}
	input, err := store.ParseInput(body)
	if err != nil {
		fail(response, http.StatusUnprocessableEntity)
		return
	}
	current, err := store.Submit(actor, input.Prompt, input.Route)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, agentwave.ErrBusy) {
			status = http.StatusConflict
		}
		if errors.Is(err, agentwave.ErrInvalid) {
			status = http.StatusUnprocessableEntity
		}
		fail(response, status)
		return
	}
	writeJob(response, current, http.StatusAccepted)
}

func readSubmitBody(response http.ResponseWriter, request *http.Request) ([]byte, bool) {
	reader := http.MaxBytesReader(response, request.Body, maxRequestBodyBytes)
	defer func() { _ = reader.Close() }()
	body, err := io.ReadAll(reader)
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			fail(response, http.StatusRequestEntityTooLarge)
		} else {
			fail(response, http.StatusBadRequest)
		}
		return nil, false
	}
	return body, true
}

func fail(response http.ResponseWriter, status int) {
	http.Error(response, http.StatusText(status), status)
}

func writeJob(response http.ResponseWriter, current agentwave.Job, status int) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(current)
}

// ListenUnix creates a Unix socket listener, replacing only an owned stale
// socket that refuses connections.
func ListenUnix(path string) (net.Listener, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("socket path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil {
		if removeErr := removeStaleSocket(path, info); removeErr != nil {
			return nil, removeErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "unix", path)
	if err != nil {
		return nil, err
	}
	//nolint:gosec // The socket needs group access so the host process can connect; 0660 is intentional.
	if err = os.Chmod(path, 0660); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

func removeStaleSocket(path string, info os.FileInfo) error {
	stat, owned := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSocket == 0 || !owned || int(stat.Uid) != os.Geteuid() {
		return errors.New("refusing to replace socket path")
	}
	dialer := net.Dialer{Timeout: socketProbeTimeout}
	connection, dialErr := dialer.DialContext(context.Background(), "unix", path)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("socket is already in use")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return errors.New("cannot establish that socket is stale")
	}
	current, statErr := os.Lstat(path)
	if statErr != nil || !os.SameFile(info, current) {
		return errors.New("socket changed during replacement")
	}
	return os.Remove(path)
}
