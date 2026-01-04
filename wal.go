package fossil

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// safe value is used to compare buf size
	// only reuse the buffer if within range
	//
	// otherwise force buffer pool to reallocate
	safeValue = 64 * 1024 // 64KB
)

type wal struct {
	f  *os.File
	mu sync.Mutex

	p *sync.Pool
}

func newWal(path string) (*wal, error) {
	clean := filepath.Clean(path)
	f, err := os.OpenFile(clean, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	bufPool := &sync.Pool{
		New: func() any {
			b := make([]byte, 1024)
			return &b
		},
	}

	return &wal{
		f:  f,
		mu: sync.Mutex{},
		p:  bufPool,
	}, nil
}

func (w *wal) append(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	s := 8 + len(key) + len(value)

	bptr := w.p.Get().(*[]byte)
	buf := *bptr

	// grow buf if needed
	if cap(buf) < s {
		buf = make([]byte, s)
	} else {
		buf = buf[:s]
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(value)))

	copy(buf[8:], key)
	copy(buf[8+len(key):], value)

	_, err := w.f.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write buf to file: %w", err)
	}

	// only return the buf to the pool if
	// buffer did not grow above safe value
	if cap(buf) <= safeValue {
		*bptr = buf
		w.p.Put(bptr)
	}

	// force os write
	return w.f.Sync()
}
