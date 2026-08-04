package main

import "sync"

// usageCallbackProcessor keeps host callbacks short while preserving the
// order in which native usage and response-interceptor callbacks are received.
// The queue is only used for SQLite-backed statistics; the in-memory path stays
// synchronous for callers that depend on immediate visibility.
type usageCallbackProcessor struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []func()
	active   bool
	stopping bool
	done     chan struct{}
}

func newUsageCallbackProcessor() *usageCallbackProcessor {
	p := &usageCallbackProcessor{done: make(chan struct{})}
	p.cond = sync.NewCond(&p.mu)
	go p.loop()
	return p
}

var usageCallbacks = newUsageCallbackProcessor()

func (p *usageCallbackProcessor) enqueue(task func()) bool {
	if p == nil || task == nil {
		return false
	}
	p.mu.Lock()
	if p.stopping {
		p.mu.Unlock()
		return false
	}
	p.queue = append(p.queue, task)
	p.cond.Signal()
	p.mu.Unlock()
	return true
}

func (p *usageCallbackProcessor) loop() {
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.stopping {
			p.cond.Wait()
		}
		if len(p.queue) == 0 && p.stopping {
			close(p.done)
			p.mu.Unlock()
			return
		}
		task := p.queue[0]
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.active = true
		p.mu.Unlock()

		task()

		p.mu.Lock()
		p.active = false
		p.cond.Broadcast()
		p.mu.Unlock()
	}
}

// drain waits until all tasks already accepted by the processor have run.
// It intentionally keeps the processor available for later callbacks.
func (p *usageCallbackProcessor) drain() {
	if p == nil {
		return
	}
	p.mu.Lock()
	for len(p.queue) > 0 || p.active {
		p.cond.Wait()
	}
	p.mu.Unlock()
}

func (p *usageCallbackProcessor) shutdown() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.stopping {
		p.stopping = true
		p.cond.Broadcast()
	}
	done := p.done
	p.mu.Unlock()
	<-done
}

func deferUsageCallback(s *RequestStatistics, task func()) bool {
	if s == nil || !s.hasEventStore() {
		return false
	}
	return usageCallbacks.enqueue(task)
}

func waitForUsageCallbacks() {
	usageCallbacks.drain()
}

func shutdownUsageCallbacks() {
	usageCallbacks.shutdown()
}

func (s *RequestStatistics) hasEventStore() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.eventStore != nil
}
