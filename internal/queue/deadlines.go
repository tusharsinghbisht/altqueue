package queue

import (
	"container/heap"
	"sort"
	"time"
)

// One entry per delayed or leased job. Extensions update in place, so heartbeats
// cannot grow an unbounded collection of stale timer entries.
type deadlineEntry struct {
	id    string
	when  time.Time
	index int
}
type deadlineQueue struct {
	items []*deadlineEntry
	byID  map[string]*deadlineEntry
}

func (h deadlineQueue) Len() int           { return len(h.items) }
func (h deadlineQueue) Less(i, j int) bool { return h.items[i].when.Before(h.items[j].when) }
func (h deadlineQueue) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *deadlineQueue) Push(v any) {
	e := v.(*deadlineEntry)
	e.index = len(h.items)
	h.items = append(h.items, e)
}
func (h *deadlineQueue) Pop() any {
	n := len(h.items) - 1
	e := h.items[n]
	h.items[n] = nil
	h.items = h.items[:n]
	return e
}
func (h *deadlineQueue) set(id string, when time.Time) {
	if h.byID == nil {
		h.byID = make(map[string]*deadlineEntry)
	}
	if e := h.byID[id]; e != nil {
		e.when = when
		heap.Fix(h, e.index)
		return
	}
	e := &deadlineEntry{id: id, when: when}
	h.byID[id] = e
	heap.Push(h, e)
}
func (h *deadlineQueue) remove(id string) {
	if e := h.byID[id]; e != nil {
		heap.Remove(h, e.index)
		delete(h.byID, id)
	}
}
func (h *deadlineQueue) due(now time.Time, limit int) []string {
	entries := make([]*deadlineEntry, 0, limit)
	var visit func(int)
	visit = func(i int) {
		if i >= len(h.items) || len(entries) >= limit || h.items[i].when.After(now) {
			return
		}
		entries = append(entries, h.items[i])
		visit(2*i + 1)
		visit(2*i + 2)
	}
	visit(0)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].when.Equal(entries[j].when) {
			return entries[i].id < entries[j].id
		}
		return entries[i].when.Before(entries[j].when)
	})
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.id
	}
	return ids
}
