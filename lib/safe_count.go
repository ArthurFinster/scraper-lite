package lib

import "sync"

// A concurrently accessible and editable counter
type SiteCounter struct {
	mu      sync.Mutex
	count   int
	blocked int
}

// create a new concurrently accessible and editable counter
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

func (s *SiteCounter) GetTotalViewed() int {
	s.mu.Lock()
	tc := s.count
	s.mu.Unlock()
	return tc
}

func (s *SiteCounter) GetTotalBlocked() int {
	s.mu.Lock()
	b := s.blocked
	s.mu.Unlock()
	return b
}
