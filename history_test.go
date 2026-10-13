//nolint:testpackage // White-box tests exercise unexported history, watcher and capacity internals.
package agentwave

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestHistoryOrderingLimitAndStatus(t *testing.T) {
	t.Parallel()

	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(_ context.Context, prompt, _ string) (Suggestion, error) {
			if prompt == "third" {
				return Suggestion{}, errors.New("provider failure")
			}
			return Suggestion{Message: prompt, Action: actionHelp, Module: moduleNone}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	for _, prompt := range []string{"first", "second", "third"} {
		if _, err = store.Submit(actorA, prompt, "/app"); err != nil {
			t.Fatal(err)
		}
		awaitFinished(t, store, actorA)
	}
	entries := store.History(actorA, 0)
	if len(entries) != 3 {
		t.Fatalf("history size: %d", len(entries))
	}
	for index, prompt := range []string{"third", "second", "first"} {
		if entries[index].Prompt != prompt || entries[index].Status == statusWorking {
			t.Fatalf("entry %d: %#v", index, entries[index])
		}
	}
	if entries[0].Status != statusFailed || entries[1].Status != statusReady || entries[2].Status != statusReady {
		t.Fatalf("statuses: %#v", entries)
	}
	for _, entry := range entries {
		if entry.FinishedAt.IsZero() {
			t.Fatalf("missing timestamp: %#v", entry)
		}
	}
	if limited := store.History(actorA, 2); len(limited) != 2 || limited[0].Prompt != "third" || limited[1].Prompt != "second" {
		t.Fatalf("limited history: %#v", limited)
	}
	if single := store.History(actorA, 1); len(single) != 1 || single[0].Prompt != "third" {
		t.Fatalf("single history: %#v", single)
	}
	if other := store.History(actorB, 0); len(other) != 0 || other == nil {
		t.Fatalf("other actor: %#v", other)
	}
	if invalid := store.History("not-a-uuid", 1); len(invalid) != 0 || invalid == nil {
		t.Fatalf("invalid actor: %#v", invalid)
	}
}

func TestHistoryCapacityEviction(t *testing.T) {
	t.Parallel()

	store, err := NewStoreWithOptions(
		context.Background(),
		testPolicy(),
		func(_ context.Context, prompt, _ string) (Suggestion, error) {
			return Suggestion{Message: prompt, Action: actionHelp, Module: moduleNone}, nil
		},
		Options{HistoryCapacity: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if store.historyCapacity != 2 {
		t.Fatalf("capacity: %d", store.historyCapacity)
	}
	for _, prompt := range []string{"one", "two", "three"} {
		if _, err = store.Submit(actorA, prompt, "/app"); err != nil {
			t.Fatal(err)
		}
		awaitFinished(t, store, actorA)
	}
	entries := store.History(actorA, 0)
	if len(entries) != 2 || entries[0].Prompt != "three" || entries[1].Prompt != "two" {
		t.Fatalf("eviction: %#v", entries)
	}
	for _, capacity := range []int{0, -3} {
		defaulted, createErr := NewStoreWithOptions(context.Background(), testPolicy(), nil, Options{HistoryCapacity: capacity})
		if createErr != nil {
			t.Fatal(createErr)
		}
		if defaulted.historyCapacity != historyLimit {
			t.Fatalf("capacity %d: %d", capacity, defaulted.historyCapacity)
		}
		defaulted.Close()
	}
}

func TestHistoryConcurrentAccess(t *testing.T) {
	t.Parallel()

	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(_ context.Context, prompt, _ string) (Suggestion, error) {
			return Suggestion{Message: prompt, Action: actionHelp, Module: moduleNone}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	var group sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 50; index++ {
				if entries := store.History(actorA, 5); len(entries) > 5 {
					t.Errorf("history over limit: %d", len(entries))
				}
				_ = store.Stats()
				_, _ = store.Current(actorB)
			}
		}()
	}
	for writer := 1; writer <= 4; writer++ {
		actor := fmt.Sprintf("%08x-2222-4222-8222-222222222222", writer)
		group.Add(1)
		go func(actor string) {
			defer group.Done()
			for index := 0; index < 4; index++ {
				if !submitAndAwait(store, actor, fmt.Sprintf("request %d", index)) {
					t.Errorf("writer %s did not finish", actor)
					return
				}
			}
		}(actor)
	}
	group.Wait()
	if entries := store.History(actorA, 0); len(entries) != 0 {
		t.Fatalf("actor A history: %#v", entries)
	}
	for writer := 1; writer <= 4; writer++ {
		actor := fmt.Sprintf("%08x-2222-4222-8222-222222222222", writer)
		if entries := store.History(actor, 0); len(entries) != 4 {
			t.Fatalf("writer %s history: %d", actor, len(entries))
		}
	}
}

func submitAndAwait(store *Store, actor, prompt string) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.Submit(actor, prompt, "/app"); err == nil {
			break
		} else if !errors.Is(err, ErrBusy) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	for time.Now().Before(deadline) {
		if current, ok := store.Current(actor); ok && current.Status != statusWorking {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestWatchJobLifecycle(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(ctx context.Context, prompt, _ string) (Suggestion, error) {
			select {
			case <-release:
				return Suggestion{Message: prompt, Action: actionHelp, Module: moduleNone}, nil
			case <-ctx.Done():
				return Suggestion{}, ctx.Err()
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	created, err := store.Submit(actorA, "Request", "/app")
	if err != nil {
		t.Fatal(err)
	}
	if watcher, _, found := store.WatchJob(actorA, "unknown"); found || watcher != nil {
		t.Fatal("unknown job accepted")
	}
	if watcher, _, found := store.WatchJob(actorB, created.ID); found || watcher != nil {
		t.Fatal("foreign job accepted")
	}
	watcher, current, found := store.WatchJob(actorA, created.ID)
	if !found || current.ID != created.ID || current.Status != statusWorking || watcher == nil {
		t.Fatalf("watch: %#v %v", current, found)
	}
	if stats := store.Stats(); stats.Watchers != 1 {
		t.Fatalf("watchers: %#v", stats)
	}
	close(release)
	update, open := <-watcher.Events()
	if !open || update.ID != created.ID || update.Status != statusReady || update.Message != "Request" {
		t.Fatalf("update: %#v %v", update, open)
	}
	if _, open = <-watcher.Events(); open {
		t.Fatal("channel stays open after a terminal update")
	}
	watcher.Close()
	if stats := store.Stats(); stats.Watchers != 0 {
		t.Fatalf("watchers after close: %#v", stats)
	}
	finished, current, found := store.WatchJob(actorA, created.ID)
	if !found || current.Status != statusReady {
		t.Fatalf("finished watch: %#v %v", current, found)
	}
	if _, open = <-finished.Events(); open {
		t.Fatal("finished job yielded an open channel")
	}
	finished.Close()
}

func TestJobWatcherDropsSlowSubscriber(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(ctx context.Context, _, _ string) (Suggestion, error) {
			select {
			case <-release:
				return Suggestion{Message: "Done", Action: actionHelp, Module: moduleNone}, nil
			case <-ctx.Done():
				return Suggestion{}, ctx.Err()
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	created, err := store.Submit(actorA, "Request", "/app")
	if err != nil {
		t.Fatal(err)
	}
	watcher, current, found := store.WatchJob(actorA, created.ID)
	if !found {
		t.Fatal("missing job")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.mu.Lock()
		defer store.mu.Unlock()
		for index := 0; index <= watcherLimit; index++ {
			store.notifyLocked(actorA, current)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification blocked on a slow subscriber")
	}
	received := 0
	for range watcher.Events() {
		received++
	}
	if received != watcherLimit {
		t.Fatalf("buffered updates: %d", received)
	}
	if stats := store.Stats(); stats.Watchers != 0 {
		t.Fatalf("dropped watcher remains: %#v", stats)
	}
	close(release)
}

func TestStoreCloseReleasesWatchers(t *testing.T) {
	t.Parallel()

	store, err := NewStore(
		context.Background(),
		testPolicy(),
		func(ctx context.Context, _, _ string) (Suggestion, error) {
			<-ctx.Done()
			return Suggestion{}, ctx.Err()
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Submit(actorA, "Request", "/app")
	if err != nil {
		t.Fatal(err)
	}
	watcher, _, found := store.WatchJob(actorA, created.ID)
	if !found {
		t.Fatal("missing job")
	}
	if stats := store.Stats(); stats.Watchers != 1 {
		t.Fatalf("watchers: %#v", stats)
	}
	watcher.Close()
	watcher.Close()
	if stats := store.Stats(); stats.Watchers != 0 {
		t.Fatalf("watchers after close: %#v", stats)
	}
	second, _, found := store.WatchJob(actorA, created.ID)
	if !found {
		t.Fatal("missing job after close")
	}
	store.Close()
	updates := 0
	for update := range second.Events() {
		if update.Status != statusFailed {
			t.Fatalf("update: %#v", update)
		}
		updates++
	}
	if updates != 1 {
		t.Fatalf("updates: %d", updates)
	}
	if stats := store.Stats(); stats.Watchers != 0 {
		t.Fatalf("watchers after store close: %#v", stats)
	}
}
