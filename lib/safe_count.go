package lib

import "sync"

// A concurrently accessible counter
type SiteCounter struct {
	mu      sync.Mutex
	count   int
	blocked int
}

func NewSiteCounter() *SiteCounter {
	return &SiteCounter{
		mu:      sync.Mutex{},
		count:   0,
		blocked: 0,
	}
}

func (s *SiteCounter) Blocked() {
	s.mu.Lock()
	s.count++
	s.blocked++
	s.mu.Unlock()
}

func (s *SiteCounter) Increment() {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
}

func (s *SiteCounter) TotalCount() int {
	s.mu.Lock()
	tc := s.count
	s.mu.Unlock()
	return tc
}

func (s *SiteCounter) TotalBlocked() int {
	s.mu.Lock()
	b := s.blocked
	s.mu.Unlock()
	return b
}
