package fossil

import (
	"errors"
	"fmt"
	"sync"
)

type DB struct {
	mu sync.RWMutex

	x, y *memtable

	w *wal

	d  string
	ss []*sstable

	memSize uint64
	flushCh chan struct{}
}

// Get
func (d *DB) Get(key []byte) ([]byte, error) {
	if val, found := d.x.get(key); found {
		return val, nil
	}

	d.mu.Lock()
	y := d.y
	d.mu.Unlock()

	if y != nil {
		if val, found := y.get(key); found {
			return val, nil
		}
	}

	for _, s := range d.ss {
		if val, found := s.get(key); found {
			return val, nil
		}
	}

	return nil, errors.New("key not found")
}

// Put
func (d *DB) Put(key, val []byte) error {
	d.mu.Lock()

	// possible flush before writing
	if d.x.getSize() >= d.memSize {
		if d.y != nil {
			d.mu.Unlock()
			return errors.New("db saturated: flush in progress")
		}
	}
	d.mu.Unlock()

	if err := d.w.append(key, val); err != nil {
		return fmt.Errorf("failed to append to wal file: %w", err)
	}

	d.x.put(key, val)

	return nil
}

func (d *DB) flush() {
	// TODO
	// move x to y memtables
	// create new wal file for new x memtable
	// spawn goroutine to write y memtable to sstable
	// discard y memtable and old wal
}
