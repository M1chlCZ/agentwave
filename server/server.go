// Package server exposes the agent assistant suggestion service exclusively
// through a private Unix socket.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/m1chlcz/agentwave"
)

// ActorHeader is the default request header that identifies the calling actor.
const ActorHeader = "X-Actor-Id"

const (
	maxRequestBodyBytes = 64 * 1024
	socketProbeTimeout  = 200 * time.Millisecond
	historyLimitDefault = 20
	historyLimitMax     = 100
)

var sseKeepAlive = 15 * time.Second

// Options configures a handler.
type Options struct {
	// ActorHeader is the request header that identifies the calling actor; an
	// empty value falls back to ActorHeader.
	ActorHeader string
	// The metrics endpoint reports Version; it defaults to "dev".
	Version string
}

// New returns a handler that reads the actor from ActorHeader.
func New(store *agentwave.Store) http.Handler {
	return NewWithOptions(store, Options{})
}

// NewWithActorHeader returns a handler that reads the actor from actorHeader;
// an empty actorHeader falls back to ActorHeader.
func NewWithActorHeader(store *agentwave.Store, actorHeader string) http.Handler {
	return NewWithOptions(store, Options{ActorHeader: actorHeader})
}

// NewWithOptions returns a handler with explicit options. The /metrics and
// /healthz endpoints do not require an actor header; they are operational
// endpoints like the socket itself. The /metrics response exposes counters
// only, never prompts, routes or suggestions.
func NewWithOptions(store *agentwave.Store, options Options) http.Handler {
	actorHeader := options.ActorHeader
	if actorHeader == "" {
		actorHeader = ActorHeader
	}
	version := options.Version
	if version == "" {
		version = "dev"
	}
	metrics := &metrics{version: version, startedAt: time.Now()}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		recorder := &statusRecorder{ResponseWriter: response}
		defer func() { metrics.observe(recorder.statusCode()) }()
		serve(store, actorHeader, metrics, recorder, request)
	})
}

func serve(
	store *agentwave.Store,
	actorHeader string,
	metrics *metrics,
	response http.ResponseWriter,
	request *http.Request,
) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if isHealthRequest(request) {
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(response, "ok\n")
		return
	}
	if isMetricsRequest(request) {
		serveMetrics(metrics, store, response, request)
		return
	}
	actor, valid := requestActor(request, actorHeader)
	if !valid {
		fail(response, http.StatusUnauthorized)
		return
	}
	switch request.URL.Path {
	case "/job":
		if rejectQuery(request, response) {
			return
		}
		serveCurrentJob(store, actor, response, request)
	case "/jobs":
		if rejectQuery(request, response) {
			return
		}
		serveSubmit(store, actor, response, request)
	case "/jobs/history":
		serveJobHistory(store, actor, response, request)
	default:
		if id, ok := eventsJobID(request.URL.Path); ok {
			serveJobEvents(store, actor, id, response, request)
			return
		}
		if rejectQuery(request, response) {
			return
		}
		fail(response, http.StatusNotFound)
	}
}

func rejectQuery(request *http.Request, response http.ResponseWriter) bool {
	if request.URL.RawQuery == "" && !request.URL.ForceQuery && request.URL.RawPath == "" {
		return false
	}
	fail(response, http.StatusUnprocessableEntity)
	return true
}

func eventsJobID(path string) (string, bool) {
	const prefix = "/jobs/"
	const suffix = "/events"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return "", false
	}
	id := path[len(prefix) : len(path)-len(suffix)]
	if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
		return "", false
	}
	return id, true
}

func isHealthRequest(request *http.Request) bool {
	return request.URL.Path == "/healthz" && request.Method == http.MethodGet && request.URL.RawQuery == ""
}

func isMetricsRequest(request *http.Request) bool {
	return request.URL.Path == "/metrics" && request.URL.RawQuery == ""
}

func serveMetrics(metrics *metrics, store *agentwave.Store, response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", "GET")
		fail(response, http.StatusMethodNotAllowed)
		return
	}
	metrics.write(store, response)
}

type metrics struct {
	version   string
	startedAt time.Time
	requests  [5]atomic.Int64
}

func (metrics *metrics) observe(status int) {
	class := status / 100
	if class < 1 || class > len(metrics.requests) {
		class = 5
	}
	metrics.requests[class-1].Add(1)
}

func (metrics *metrics) write(store *agentwave.Store, response http.ResponseWriter) {
	stats := store.Stats()
	response.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var builder strings.Builder
	builder.WriteString("# HELP agentwave_build_info Build information.\n")
	builder.WriteString("# TYPE agentwave_build_info gauge\n")
	fmt.Fprintf(&builder, "agentwave_build_info{version=%q} 1\n", metrics.version)
	builder.WriteString("# HELP agentwave_jobs_started_total Admitted generations.\n")
	builder.WriteString("# TYPE agentwave_jobs_started_total counter\n")
	fmt.Fprintf(&builder, "agentwave_jobs_started_total %d\n", stats.StartedGenerations)
	builder.WriteString("# HELP agentwave_jobs_completed_total Generations that produced a ready suggestion.\n")
	builder.WriteString("# TYPE agentwave_jobs_completed_total counter\n")
	fmt.Fprintf(&builder, "agentwave_jobs_completed_total %d\n", stats.ReadyGenerations)
	builder.WriteString("# HELP agentwave_jobs_failed_total Generations that failed.\n")
	builder.WriteString("# TYPE agentwave_jobs_failed_total counter\n")
	fmt.Fprintf(&builder, "agentwave_jobs_failed_total %d\n", stats.FailedGenerations)
	builder.WriteString("# HELP agentwave_jobs_in_flight Generations currently running.\n")
	builder.WriteString("# TYPE agentwave_jobs_in_flight gauge\n")
	fmt.Fprintf(&builder, "agentwave_jobs_in_flight %d\n", stats.ActiveGenerations)
	builder.WriteString("# HELP agentwave_entries_live Live suggestion entries.\n")
	builder.WriteString("# TYPE agentwave_entries_live gauge\n")
	fmt.Fprintf(&builder, "agentwave_entries_live %d\n", stats.LiveEntries)
	builder.WriteString("# HELP agentwave_requests_total HTTP responses by status class.\n")
	builder.WriteString("# TYPE agentwave_requests_total counter\n")
	for class := range metrics.requests {
		fmt.Fprintf(
			&builder,
			"agentwave_requests_total{class=%q} %d\n",
			fmt.Sprintf("%dxx", class+1),
			metrics.requests[class].Load(),
		)
	}
	builder.WriteString("# HELP agentwave_uptime_seconds Seconds since start.\n")
	builder.WriteString("# TYPE agentwave_uptime_seconds gauge\n")
	fmt.Fprintf(&builder, "agentwave_uptime_seconds %.3f\n", time.Since(metrics.startedAt).Seconds())
	_, _ = io.WriteString(response, builder.String())
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	if recorder.status == 0 {
		recorder.status = status
	}
	recorder.ResponseWriter.WriteHeader(status)
}

func (recorder *statusRecorder) Write(body []byte) (int, error) {
	if recorder.status == 0 {
		recorder.status = http.StatusOK
	}
	return recorder.ResponseWriter.Write(body)
}

func (recorder *statusRecorder) Flush() {
	if flusher, ok := recorder.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (recorder *statusRecorder) statusCode() int {
	if recorder.status == 0 {
		return http.StatusOK
	}
	return recorder.status
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
	input, err := store.ParseSubmit(body)
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

func serveJobHistory(store *agentwave.Store, actor string, response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", "GET")
		fail(response, http.StatusMethodNotAllowed)
		return
	}
	limit, ok := parseHistoryLimit(request)
	if !ok {
		fail(response, http.StatusUnprocessableEntity)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(store.History(actor, limit))
}

func parseHistoryLimit(request *http.Request) (int, bool) {
	if request.URL.ForceQuery || request.URL.RawPath != "" {
		return 0, false
	}
	if request.URL.RawQuery == "" {
		return historyLimitDefault, true
	}
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || len(values) != 1 || len(values["limit"]) != 1 {
		return 0, false
	}
	limit, err := strconv.Atoi(values["limit"][0])
	if err != nil || limit < 1 || limit > historyLimitMax {
		return 0, false
	}
	return limit, true
}

func serveJobEvents(store *agentwave.Store, actor, id string, response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", "GET")
		fail(response, http.StatusMethodNotAllowed)
		return
	}
	once, ok := parseEventsQuery(request)
	if !ok {
		fail(response, http.StatusUnprocessableEntity)
		return
	}
	watcher, current, found := store.WatchJob(actor, id)
	if !found {
		fail(response, http.StatusNotFound)
		return
	}
	defer watcher.Close()
	response.Header().Set("Content-Type", "text/event-stream")
	writeJobEvent(response, current)
	flush(response)
	if once {
		return
	}
	keepAlive := time.NewTicker(sseKeepAlive)
	defer keepAlive.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case update, open := <-watcher.Events():
			if !open {
				return
			}
			writeJobEvent(response, update)
			flush(response)
		case <-keepAlive.C:
			_, _ = io.WriteString(response, ": keep-alive\n\n")
			flush(response)
		}
	}
}

func parseEventsQuery(request *http.Request) (bool, bool) {
	if request.URL.ForceQuery || request.URL.RawPath != "" {
		return false, false
	}
	if request.URL.RawQuery == "" {
		return false, true
	}
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || len(values) != 1 || len(values["once"]) != 1 || values["once"][0] != "1" {
		return false, false
	}
	return true, true
}

func writeJobEvent(response http.ResponseWriter, current agentwave.Job) {
	payload, _ := json.Marshal(current)
	_, _ = fmt.Fprintf(response, "event: %s\ndata: %s\n\n", current.Status, payload)
}

func flush(response http.ResponseWriter) {
	if flusher, ok := response.(http.Flusher); ok {
		flusher.Flush()
	}
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
	if info.Mode()&os.ModeSocket == 0 || !ownedSocket(info) {
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
