package lib

import "sync"

// A concurrently accessible map
type VisitedMap struct {
	visited map[string]bool
	mu      sync.Mutex
}

func NewVisitedMap() *VisitedMap {
	return &VisitedMap{
		visited: make(map[string]bool),
		mu:      sync.Mutex{},
	}
}

func (v *VisitedMap) Visited(url string) {
	v.mu.Lock()
	v.visited[url] = true
	v.mu.Unlock()
}

func (v *VisitedMap) WasVisited(url string) bool {
	v.mu.Lock()
	r := v.visited[url]
	v.mu.Unlock()
	return r
}
