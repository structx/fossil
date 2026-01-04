package fossil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	maxLevel = 16
)

type node struct {
	key, val []byte
	next     []*node
}

type memtable struct {
	head   *node // senteniel head should store full tower of pointers
	height int

	mu sync.Mutex

	size uint64

	u *sync.Pool
	b *sync.Pool
}

// memtable is a skip list
func newMemtable() *memtable {
	return &memtable{
		head:   &node{next: make([]*node, maxLevel)},
		height: 1,
		mu:     sync.Mutex{},
		u: &sync.Pool{
			New: func() any {
				return new([maxLevel]*node)
			},
		},
		b: &sync.Pool{
			New: func() any {
				bb := make([]byte, 1024)
				return &bb
			},
		},
	}
}

func (m *memtable) get(key []byte) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := m.head
	for i := m.height - 1; i >= 0; i-- {
		for c.next[i] != nil && bytes.Compare(c.next[i].key, key) < 0 {
			c = c.next[i]
		}
	}

	c = c.next[0]

	if c != nil && bytes.Equal(c.key, key) {
		return c.val, true
	}

	return nil, false
}

func (m *memtable) put(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p := m.u.Get().(*[maxLevel]*node)
	n := *p
	defer m.u.Put(n)

	c := m.head

	// start at highest level
	for i := m.height - 1; i >= 0; i-- {
		// moves current pointer all the way right
		// stay on current level as long as next is not nil and key is less than insert key
		for c.next[i] != nil && bytes.Compare(c.next[i].key, key) < 0 {
			c = c.next[i]
		}
		// left neighbor of our new node at this level
		n[i] = c
	}

	c = c.next[0]

	// override existing value
	if c != nil && bytes.Equal(c.key, key) {
		c.val = val
		return
	}

	h := randomHeight()
	if h > maxLevel {
		for i := m.height; i < maxLevel; i++ {
			n[i] = m.head
		}
		m.height = h
	}

	nn := &node{
		key:  key,
		val:  val,
		next: make([]*node, h),
	}

	for i := range h {
		nn.next[i] = n[i].next[i]
		n[i].next[i] = nn
	}
}

func (m *memtable) flush(path string) error {
	clean := filepath.Clean(path)
	f, err := os.Create(clean)
	if err != nil {
		return fmt.Errorf("os.Create: %w", err)
	}
	defer f.Close()

	c := m.head.next[0]

	// iterate over level 0
	// this will be the full sorted list
	for c != nil {
		b := make([]byte, 8+len(c.key)+len(c.val))
		binary.BigEndian.PutUint32(b[0:4], uint32(len(c.key)))
		binary.BigEndian.PutUint32(b[4:8], uint32(len(c.val)))

		copy(b[:8], c.key)
		copy(b[:8+len(c.key)], c.val)

		if _, err := f.Write(b); err != nil {
			return fmt.Errorf("f.Write: %w", err)
		}
		c = c.next[0]
	}

	return f.Sync()
}

func (m *memtable) getSize() uint64 {
	return m.size
}

func randomHeight() int {
	h := 1
	for h < maxLevel && (uint32(time.Now().UnixNano())%2 == 0) {
		h++
	}
	return h
}
