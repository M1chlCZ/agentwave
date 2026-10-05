// Package agentwave implements the policy engine and job store of an embeddable
// agent assistant. It stores short-lived, schema-validated suggestions and runs
// one sandboxed generation per actor. It has no application data or mutation
// interface.
package agentwave

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	entryLimit        = 32
	generationLimit   = 2
	entryTTL          = 15 * time.Minute
	generationTimeout = 120 * time.Second
	messageLimit      = 2000
	queryLimit        = 120
	routeLimit        = 2048
	inputLimit        = 64 * 1024
	suggestionLimit   = 32 * 1024
	defaultWorking    = "Preparing a suggestion."
	defaultFailed     = "The suggestion could not be prepared. Try again."
	statusWorking     = "working"
	statusReady       = "ready"
	statusFailed      = "failed"
	actionHelp        = "help"
	actionNavigate    = "navigate"
	moduleNone        = "none"
)

// ErrInvalid reports malformed input or a suggestion outside the policy.
var ErrInvalid = errors.New("invalid input")

// ErrBusy reports that the actor or the service is already generating.
var ErrBusy = errors.New("generation busy")

// ErrCapacity reports that the store cannot admit another entry.
var ErrCapacity = errors.New("unavailable")

var (
	actorPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	namePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// Policy describes the suggestion vocabulary, route scope and messages that a
// Store or a generation backend accepts.
type Policy struct {
	// Actions lists the allowed suggestion actions; "help" and "navigate" are reserved.
	Actions []string
	// Modules lists the allowed suggestion modules; it must contain "none".
	Modules []string
	// RoutePrefix scopes accepted routes; the empty value defaults to "/".
	RoutePrefix string
	// QueryActions maps an action to the module it requires with a non-empty query.
	QueryActions map[string]string
	// Working is the message of an admitted job; it defaults to a generic message.
	Working string
	// Failed is the message of a failed job; it defaults to a generic message.
	Failed string
}

// Input is one validated request for a suggestion.
type Input struct {
	Prompt string `json:"prompt"`
	Route  string `json:"route"`
}

// Suggestion is one validated suggestion produced for an input.
type Suggestion struct {
	Message string `json:"message"`
	Action  string `json:"action"`
	Module  string `json:"module"`
	Query   string `json:"query"`
}

// Job is the current state of an actor's suggestion.
type Job struct {
	Suggestion

	ID     string `json:"id"`
	Status string `json:"status"`
	Prompt string `json:"prompt"`
	Route  string `json:"route"`
}

// Generator produces one suggestion for a prompt and route.
type Generator func(context.Context, string, string) (Suggestion, error)

type entry struct {
	job     Job
	expires time.Time
}

// Store holds at most one live job per actor under a fixed policy.
type Store struct {
	mu       sync.Mutex
	entries  map[string]entry
	active   int
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	workers  sync.WaitGroup
	generate Generator
	policy   Policy
	now      func() time.Time
	timeout  time.Duration
}

// NewStore validates the policy and returns a store whose generations run
// under parent.
func NewStore(parent context.Context, policy Policy, generate Generator) (*Store, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	return &Store{
		entries:  make(map[string]entry),
		ctx:      ctx,
		cancel:   cancel,
		generate: generate,
		policy:   policy.Normalized(),
		now:      time.Now,
		timeout:  generationTimeout,
	}, nil
}

// ParseInput decodes one strict JSON input document with the store policy.
func (store *Store) ParseInput(raw []byte) (Input, error) {
	return store.policy.ParseInput(raw)
}

// Submit validates the request and admits one job for the actor.
func (store *Store) Submit(actor, prompt, route string) (Job, error) {
	input, err := store.policy.ValidateInput(prompt, route)
	if err != nil || !ValidActor(actor) {
		return Job{}, ErrInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.expireLocked()
	if store.closed || store.ctx.Err() != nil || store.generate == nil {
		return Job{}, ErrCapacity
	}
	previous, exists := store.entries[actor]
	if previous.job.Status == statusWorking || store.active >= generationLimit {
		return Job{}, ErrBusy
	}
	if !exists && len(store.entries) >= entryLimit {
		return Job{}, ErrCapacity
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return Job{}, ErrCapacity
	}
	current := Job{
		ID:      hex.EncodeToString(id[:]),
		Status:  statusWorking,
		Prompt:  input.Prompt,
		Route:   input.Route,
		Message: store.policy.Working,
		Action:  actionHelp,
		Module:  moduleNone,
	}
	store.entries[actor] = entry{job: current, expires: store.now().Add(entryTTL)}
	store.active++
	store.workers.Add(1)
	go store.run(actor, current)
	return current, nil
}

func (store *Store) run(actor string, current Job) {
	defer store.workers.Done()
	ctx, cancel := context.WithTimeout(store.ctx, store.timeout)
	defer cancel()
	var suggestion Suggestion
	var err error
	defer func() {
		if recover() != nil {
			err = ErrCapacity
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		store.active--
		current.Status = statusFailed
		current.Suggestion = Suggestion{Message: store.policy.Failed, Action: actionHelp, Module: moduleNone}
		if err == nil && ctx.Err() == nil && store.policy.ValidateSuggestion(suggestion) == nil {
			current.Status = statusReady
			current.Suggestion = suggestion
		}
		if saved, ok := store.entries[actor]; ok && saved.job.ID == current.ID {
			saved.job = current
			store.entries[actor] = saved
		}
	}()
	suggestion, err = store.generate(ctx, current.Prompt, current.Route)
}

// Current returns the actor's live job, if any.
func (store *Store) Current(actor string) (Job, bool) {
	if !ValidActor(actor) {
		return Job{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.expireLocked()
	current, ok := store.entries[actor]
	return current.job, ok
}

func (store *Store) expireLocked() {
	now := store.now()
	for actor, current := range store.entries {
		if current.job.Status != statusWorking && !now.Before(current.expires) {
			delete(store.entries, actor)
		}
	}
}

// Close stops the store and waits for running generations.
func (store *Store) Close() {
	store.mu.Lock()
	store.closed = true
	store.cancel()
	store.mu.Unlock()
	store.workers.Wait()
}

// ValidActor reports whether actor is a non-nil lowercase UUID.
func ValidActor(actor string) bool {
	return actorPattern.MatchString(actor) && actor != "00000000-0000-0000-0000-000000000000"
}

// ValidateInput trims the prompt, validates the route and returns the input.
func (policy Policy) ValidateInput(prompt, route string) (Input, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" || !utf8.ValidString(prompt) || utf8.RuneCountInString(prompt) > messageLimit ||
		strings.ContainsRune(prompt, '\x00') ||
		!policy.validRoute(route) {
		return Input{}, ErrInvalid
	}
	return Input{Prompt: prompt, Route: route}, nil
}

// ParseInput decodes one strict JSON input document and validates it.
func (policy Policy) ParseInput(raw []byte) (Input, error) {
	values, err := flatStrings(raw, inputLimit, "prompt", "route")
	if err != nil {
		return Input{}, err
	}
	return policy.ValidateInput(values["prompt"], values["route"])
}

// ParseSuggestion decodes one strict JSON suggestion document and validates it.
func (policy Policy) ParseSuggestion(raw []byte) (Suggestion, error) {
	values, err := flatStrings(raw, suggestionLimit, "message", "action", "module", "query")
	if err != nil {
		return Suggestion{}, err
	}
	suggestion := Suggestion{
		Message: values["message"],
		Action:  values["action"],
		Module:  values["module"],
		Query:   values["query"],
	}
	if err = policy.ValidateSuggestion(suggestion); err != nil {
		return Suggestion{}, err
	}
	return suggestion, nil
}

// ValidateSuggestion reports whether a suggestion matches the policy
// vocabulary and the reserved action rules.
func (policy Policy) ValidateSuggestion(suggestion Suggestion) error {
	if !utf8.ValidString(suggestion.Message) || strings.TrimSpace(suggestion.Message) == "" ||
		utf8.RuneCountInString(suggestion.Message) > messageLimit ||
		!utf8.ValidString(suggestion.Query) ||
		len(suggestion.Query) > queryLimit ||
		strings.TrimSpace(suggestion.Query) != suggestion.Query ||
		strings.ContainsFunc(suggestion.Query, unicode.IsControl) {
		return ErrInvalid
	}
	if !slices.Contains(policy.Actions, suggestion.Action) || !slices.Contains(policy.Modules, suggestion.Module) {
		return ErrInvalid
	}
	switch suggestion.Action {
	case actionHelp:
		if suggestion.Query == "" {
			return nil
		}
	case actionNavigate:
		if suggestion.Module != moduleNone && suggestion.Query == "" {
			return nil
		}
	default:
		if required, ok := policy.QueryActions[suggestion.Action]; ok && suggestion.Module == required &&
			suggestion.Query != "" {
			return nil
		}
	}
	return ErrInvalid
}

// Validate reports whether the policy lists a usable suggestion vocabulary.
func (policy Policy) Validate() error {
	if err := validateNames("actions", policy.Actions); err != nil {
		return err
	}
	if err := validateNames("modules", policy.Modules); err != nil {
		return err
	}
	if !slices.Contains(policy.Actions, actionHelp) {
		return errors.New("policy actions must contain \"help\"")
	}
	if !slices.Contains(policy.Modules, moduleNone) {
		return errors.New("policy modules must contain \"none\"")
	}
	for action, module := range policy.QueryActions {
		if action == actionHelp || action == actionNavigate {
			return fmt.Errorf("policy query action %q is reserved", action)
		}
		if !slices.Contains(policy.Actions, action) {
			return fmt.Errorf("policy query action %q is not listed in actions", action)
		}
		if !slices.Contains(policy.Modules, module) {
			return fmt.Errorf("policy query action %q targets unlisted module %q", action, module)
		}
	}
	if prefix := policy.RoutePrefix; prefix != "" {
		if !strings.HasPrefix(prefix, "/") {
			return errors.New("policy route prefix must start with a slash")
		}
		if prefix != "/" && strings.HasSuffix(prefix, "/") {
			return errors.New("policy route prefix must not end with a slash")
		}
	}
	return nil
}

// Normalized returns a copy with the default route prefix and messages.
func (policy Policy) Normalized() Policy {
	if policy.RoutePrefix == "" {
		policy.RoutePrefix = "/"
	}
	if policy.Working == "" {
		policy.Working = defaultWorking
	}
	if policy.Failed == "" {
		policy.Failed = defaultFailed
	}
	return policy
}

func (policy Policy) validRoute(route string) bool {
	if len(route) > routeLimit || !utf8.ValidString(route) || strings.Contains(route, "\\") {
		return false
	}
	parsed, err := url.Parse(route)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" ||
		parsed.RawPath != "" ||
		parsed.Path != route ||
		parsed.EscapedPath() != route ||
		path.Clean(route) != route {
		return false
	}
	prefix := policy.RoutePrefix
	if prefix == "" {
		prefix = "/"
	}
	if prefix == "/" {
		return strings.HasPrefix(route, "/")
	}
	return route == prefix || strings.HasPrefix(route, prefix+"/")
}

func validateNames(kind string, names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("policy %s must not be empty", kind)
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("policy %s entry %q is not a lowercase name", kind, name)
		}
		if seen[name] {
			return fmt.Errorf("policy %s entry %q is duplicated", kind, name)
		}
		seen[name] = true
	}
	return nil
}

// flatStrings parses an object of string fields and rejects duplicates, nulls,
// missing fields, nested values and trailing JSON rather than normalizing them.
func flatStrings(raw []byte, limit int, keys ...string) (map[string]string, error) {
	if len(raw) > limit || !utf8.Valid(raw) {
		return nil, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, ErrInvalid
	}
	allowed := make(map[string]bool, len(keys))
	for _, key := range keys {
		allowed[key] = true
	}
	values := make(map[string]string, len(keys))
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return nil, ErrInvalid
		}
		key, ok := keyToken.(string)
		if !ok || !allowed[key] {
			return nil, ErrInvalid
		}
		if _, duplicate := values[key]; duplicate {
			return nil, ErrInvalid
		}
		valueToken, valueErr := decoder.Token()
		if valueErr != nil {
			return nil, ErrInvalid
		}
		value, ok := valueToken.(string)
		if !ok {
			return nil, ErrInvalid
		}
		values[key] = value
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || len(values) != len(keys) {
		return nil, ErrInvalid
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, ErrInvalid
	}
	return values, nil
}
