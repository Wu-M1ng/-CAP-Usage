package main

import "sync"

const (
	// Response interceptor payloads can include the complete stream history.
	// Bound both task count and retained payload bytes so a slow SQLite writer
	// cannot turn callback traffic into unbounded heap growth.
	usageCallbackQueueMaxTasks = 256
	usageCallbackQueueMaxBytes = 16 << 20
	usageCallbackTaskOverhead  = 1024
)

// usageCallbackProcessor keeps host callbacks short while preserving the
// order in which native usage and response-interceptor callbacks are received.
// The queue is only used for SQLite-backed statistics; the in-memory path stays
// synchronous for callers that depend on immediate visibility.
type usageCallbackProcessor struct {
	mu          sync.Mutex
	cond        *sync.Cond
	queue       []usageCallbackTask
	queuedBytes int
	active      bool
	stopping    bool
	done        chan struct{}
}

type usageCallbackTask struct {
	fn       func()
	retained int
}

func newUsageCallbackProcessor() *usageCallbackProcessor {
	p := &usageCallbackProcessor{done: make(chan struct{})}
	p.cond = sync.NewCond(&p.mu)
	go p.loop()
	return p
}

var usageCallbacks = newUsageCallbackProcessor()

func (p *usageCallbackProcessor) enqueue(task func(), payloadBytes int) bool {
	if p == nil || task == nil {
		return false
	}
	if payloadBytes < 0 {
		payloadBytes = 0
	}
	retainedBytes := payloadBytes + usageCallbackTaskOverhead
	if retainedBytes < payloadBytes {
		retainedBytes = usageCallbackQueueMaxBytes + 1
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping {
		return false
	}
	fits := len(p.queue) < usageCallbackQueueMaxTasks &&
		(retainedBytes <= usageCallbackQueueMaxBytes-p.queuedBytes)
	if !fits {
		// Host callbacks must never wait for SQLite or an earlier callback. The
		// caller records an overflow metric and returns the empty success envelope.
		return false
	}
	p.queue = append(p.queue, usageCallbackTask{fn: task, retained: retainedBytes})
	p.queuedBytes += retainedBytes
	p.cond.Signal()
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
		p.queue[0] = usageCallbackTask{}
		p.queue = p.queue[1:]
		p.queuedBytes -= task.retained
		if len(p.queue) == 0 {
			p.queue = nil
			p.queuedBytes = 0
		}
		p.active = true
		p.cond.Broadcast()
		p.mu.Unlock()

		task.fn()

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

type usageCallbackDisposition uint8

const (
	usageCallbackSync usageCallbackDisposition = iota
	usageCallbackQueued
	usageCallbackDropped
)

func deferUsageCallback(s *RequestStatistics, task func(), payloadBytes int) usageCallbackDisposition {
	if s == nil || !s.hasEventStore() {
		return usageCallbackSync
	}
	if usageCallbacks.enqueue(task, payloadBytes) {
		return usageCallbackQueued
	}
	s.recordUsageCallbackDrop()
	return usageCallbackDropped
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
