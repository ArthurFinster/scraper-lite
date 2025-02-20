package lib

import (
	"sync"
)

// A concurrently accessible map
type SiteMap struct {
	visited   map[string]bool
	collected []Url
	mu        sync.Mutex
}

func NewSiteMap() *SiteMap {
	return &SiteMap{
		visited:   make(map[string]bool),
		collected: []Url{},
		mu:        sync.Mutex{},
	}
}

func (v *SiteMap) GetLinks() []Url {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.collected
}

func (v *SiteMap) AppendPdf(u Url) {
	v.mu.Lock()
	v.collected = append(v.collected, u)
	v.visited[u.PdfUrl] = true
	v.mu.Unlock()
}

// TODO: this file needs a rename
func (v *SiteMap) Visited(url string) {
	v.mu.Lock()
	v.visited[url] = true
	v.mu.Unlock()
}

func (v *SiteMap) WasVisited(url string) bool {
	v.mu.Lock()
	r := v.visited[url]
	v.mu.Unlock()
	return r
}
