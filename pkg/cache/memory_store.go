package cache

import (
	"context"
	"strings"
	"sync"
)

type memory struct {
	l       sync.RWMutex
	records map[string]Record
}

func NewMemoryStore() Store {
	return &memory{
		records: make(map[string]Record),
	}
}

func (mem *memory) Lock(ctx context.Context) (UnlockFunc, bool) {
	mem.l.Lock()
	return func() {
		mem.l.Unlock()
	}, true
}

func (mem *memory) Put(_ context.Context, r KeyRecord) error {
	mem.records[r.Key] = cloneRecord(r.Record)
	return nil
}

func (mem *memory) Get(_ context.Context, key string) (*KeyRecord, error) {
	r, ok := mem.records[key]
	if !ok {
		return nil, nil
	}

	c := cloneRecord(&r)
	return &KeyRecord{
		Key:    key,
		Record: &c,
	}, nil
}

func (mem *memory) Delete(_ context.Context, key string) error {
	delete(mem.records, key)
	return nil
}

func (mem *memory) List(_ context.Context, prefix string) ([]*KeyRecord, error) {
	var res []*KeyRecord
	for key, r := range mem.records {
		if strings.HasPrefix(key, prefix) {
			c := cloneRecord(&r)
			res = append(res, &KeyRecord{
				Key:    key,
				Record: &c,
			})
		}
	}
	return res, nil
}
