package buffer

import (
	"errors"
	"io"
	"time"
)

var (
	// ErrTimeout indicates an operation has timed out.
	ErrTimeout = errors.New("operation timed-out")
	// ErrClosed indicates the buffer is closed and can no longer be used.
	ErrClosed = errors.New("buffer is closed")
)

type (
	// Buffer represents a data buffer that is asynchronously flushed, either manually or automatically.
	Buffer struct {
		io.Closer
		dataCh  chan interface{}
		flushCh chan struct{}
		clearCh chan struct{}
		closeCh chan struct{}
		doneCh  chan struct{}

		// buffer to store items and map to help quick search items
		items         []interface{}
		itemsQueryMap map[interface{}]struct{}
		count         int

		// ticker
		ticker     <-chan time.Time
		stopTicker func()

		options *Options
	}
)

// Push appends an item to the end of the buffer.
//
// It returns an ErrTimeout if if cannot be performed in a timely fashion, and
// an ErrClosed if the buffer has been closed.
func (buffer *Buffer) Push(item interface{}) error {
	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.dataCh <- item:
		return nil
	case <-time.After(buffer.options.PushTimeout):
		return ErrTimeout
	}
}

// Flush outputs the buffer to a permanent destination.
//
// It returns an ErrTimeout if if cannot be performed in a timely fashion, and
// an ErrClosed if the buffer has been closed.
func (buffer *Buffer) Flush() error {
	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.flushCh <- struct{}{}:
		return nil
	case <-time.After(buffer.options.FlushTimeout):
		return ErrTimeout
	}
}

// Clear clears the buffer.
//
// It returns an ErrClosed if the buffer has been closed.
func (buffer *Buffer) Clear() error {
	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.clearCh <- struct{}{}:
		return nil
	case <-time.After(buffer.options.ClearTimeout):
		return ErrTimeout
	}
}

// Close flushes the buffer and prevents it from being further used.
//
// It returns an ErrTimeout if if cannot be performed in a timely fashion, and
// an ErrClosed if the buffer has already been closed.
//
// An ErrTimeout can either mean that a flush could not be triggered, or it can
// mean that a flush was triggered but it has not finished yet. In any case it is
// safe to call Close again.
func (buffer *Buffer) Close() error {
	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.closeCh <- struct{}{}:
		// noop
	case <-time.After(buffer.options.CloseTimeout):
		return ErrTimeout
	}

	select {
	case <-buffer.doneCh:
		close(buffer.dataCh)
		close(buffer.flushCh)
		close(buffer.closeCh)
		return nil
	case <-time.After(buffer.options.CloseTimeout):
		return ErrTimeout
	}
}

// Count returns the number of items in the buffer.
//
// It returns an ErrClosed if the buffer has been closed.
func (buffer *Buffer) Count() (int, error) {
	if buffer.closed() {
		return 0, ErrClosed
	}

	return buffer.count, nil
}

// Exists checks if an item exists in the buffer.
//
// It returns an ErrClosed if the buffer has been closed.
func (buffer *Buffer) Exists(item interface{}) (bool, error) {
	if buffer.closed() {
		return false, ErrClosed
	}

	_, ok := buffer.itemsQueryMap[item]
	return ok, nil
}

func (buffer Buffer) closed() bool {
	select {
	case <-buffer.doneCh:
		return true
	default:
		return false
	}
}

func (buffer *Buffer) consume() {
	mustFlush := false
	buffer.ticker, buffer.stopTicker = newTicker(buffer.options.FlushInterval)

	isOpen := true
	for isOpen {
		select {
		case item := <-buffer.dataCh:
			buffer.items[buffer.count] = item
			buffer.itemsQueryMap[item] = struct{}{}
			buffer.count++
			mustFlush = buffer.count >= len(buffer.items)
		case <-buffer.ticker:
			mustFlush = buffer.count > 0
		case <-buffer.clearCh:
			buffer.stopTicker()
			buffer.resetItems()
			mustFlush = false
			buffer.ticker, buffer.stopTicker = newTicker(buffer.options.FlushInterval)
		case <-buffer.flushCh:
			mustFlush = buffer.count > 0
		case <-buffer.closeCh:
			isOpen = false
			mustFlush = buffer.count > 0
		}

		if mustFlush {
			buffer.stopTicker()
			buffer.options.Flusher.Write(buffer.items[:buffer.count])

			buffer.resetItems()
			mustFlush = false

			buffer.ticker, buffer.stopTicker = newTicker(buffer.options.FlushInterval)
		}
	}

	buffer.stopTicker()
	close(buffer.doneCh)
}

func (buffer *Buffer) resetItems() {
	buffer.count = 0
	buffer.items = make([]interface{}, buffer.options.Size)
	buffer.itemsQueryMap = make(map[interface{}]struct{})
}

func newTicker(interval time.Duration) (<-chan time.Time, func()) {
	if interval == 0 {
		return nil, func() {}
	}

	ticker := time.NewTicker(interval)
	return ticker.C, ticker.Stop
}

// New creates a new buffer instance with the provided options.
func New(opts ...Option) *Buffer {
	buffer := &Buffer{
		dataCh:  make(chan interface{}),
		flushCh: make(chan struct{}),
		clearCh: make(chan struct{}),
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
		options: resolveOptions(opts...),
	}

	// init itemBuffer and quickSearchMap
	buffer.items = make([]interface{}, buffer.options.Size)
	buffer.itemsQueryMap = make(map[interface{}]struct{})

	go buffer.consume()

	return buffer
}
