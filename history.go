package agentwave

import "time"

// HistoryEntry is one completed job in the store history. It carries the same
// fields as a job response plus the time when the job reached its terminal
// status.
type HistoryEntry struct {
	Job
	FinishedAt time.Time `json:"finishedAt"`
}

type historyRecord struct {
	actor string
	entry HistoryEntry
}

// History returns up to limit of the actor's most recent completed jobs, most
// recent first. A limit of zero or less returns every retained entry. The
// history is bounded by the store capacity and never contains jobs of another
// actor.
func (store *Store) History(actor string, limit int) []HistoryEntry {
	if !ValidActor(actor) {
		return []HistoryEntry{}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.expireLocked()
	entries := make([]HistoryEntry, 0)
	for index := len(store.history) - 1; index >= 0; index-- {
		record := store.history[index]
		if record.actor != actor {
			continue
		}
		entries = append(entries, record.entry)
		if limit > 0 && len(entries) >= limit {
			break
		}
	}
	return entries
}

func (store *Store) recordLocked(actor string, current Job) {
	if store.historyCapacity <= 0 {
		return
	}
	record := historyRecord{actor: actor, entry: HistoryEntry{Job: current, FinishedAt: store.now()}}
	if len(store.history) < store.historyCapacity {
		store.history = append(store.history, record)
		return
	}
	copy(store.history, store.history[1:])
	store.history[len(store.history)-1] = record
}
