package agentwave

// JobWatcher streams the state transitions of one job to a subscriber. The
// store closes the events channel when the job reaches a terminal status, when
// the subscriber falls behind, or when the store closes.
type JobWatcher struct {
	store  *Store
	actor  string
	id     string
	events chan Job
	closed bool
}

// Events returns the channel of job updates.
func (watcher *JobWatcher) Events() <-chan Job {
	return watcher.events
}

// Close unsubscribes the watcher. Calling it more than once is safe. The store
// owns the events channel and closes it.
func (watcher *JobWatcher) Close() {
	store := watcher.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closeWatcherLocked(watcher)
}

// WatchJob returns a watcher and the current state of the actor's live job
// with the given id. found is false when the actor has no live job with that
// id. A finished job yields a watcher whose channel is already closed.
func (store *Store) WatchJob(actor, id string) (*JobWatcher, Job, bool) {
	if !ValidActor(actor) || id == "" {
		return nil, Job{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.expireLocked()
	saved, ok := store.entries[actor]
	if !ok || saved.job.ID != id {
		return nil, Job{}, false
	}
	watcher := &JobWatcher{store: store, actor: actor, id: id, events: make(chan Job, watcherLimit)}
	if saved.job.Status != statusWorking || store.closed {
		close(watcher.events)
		watcher.closed = true
		return watcher, saved.job, true
	}
	if store.watchers[actor] == nil {
		store.watchers[actor] = make(map[*JobWatcher]struct{})
	}
	store.watchers[actor][watcher] = struct{}{}
	return watcher, saved.job, true
}

func (store *Store) notifyLocked(actor string, current Job) {
	for watcher := range store.watchers[actor] {
		if watcher.id != current.ID {
			continue
		}
		select {
		case watcher.events <- current:
			if current.Status != statusWorking {
				store.closeWatcherLocked(watcher)
			}
		default:
			store.closeWatcherLocked(watcher)
		}
	}
}

func (store *Store) closeWatcherLocked(watcher *JobWatcher) {
	if watcher.closed {
		return
	}
	watcher.closed = true
	close(watcher.events)
	registered := store.watchers[watcher.actor]
	delete(registered, watcher)
	if len(registered) == 0 {
		delete(store.watchers, watcher.actor)
	}
}

func (store *Store) closeWatchersLocked() {
	for _, registered := range store.watchers {
		for watcher := range registered {
			store.closeWatcherLocked(watcher)
		}
	}
}
