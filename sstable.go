package fossil

import "os"

type sstable struct {
	f     *os.File
	index map[string]int64
}

func (ss *sstable) get(key []byte) ([]byte, bool) {
	// TODO
	// implement handler
	return nil, false
}
