package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tierklinik-dobersberg/logger"
)

type Metadata struct {
	NotValidAfter    *time.Time
	CreatedAt        time.Time
	BurnAfterReading bool
}

func (md *Metadata) IsValid() bool {
	if md.NotValidAfter == nil {
		return true
	}
	return md.NotValidAfter.After(time.Now())
}

// Record represents a cache record stored under a certain key.
// Users of Cache should not care about the Record struct as it'
// mainly exported for Store interface implementers.
type Record struct {
	Metadata
	Data []byte
}

type Cache interface {
	// Write stores data under key.
	Write(ctx context.Context, key string, data []byte, opts ...Option) error

	// Read returns the data and associated metadata stored under key.
	Read(ctx context.Context, key string) ([]byte, *Metadata, error)

	// Delete deletes any data stored under key.
	Delete(ctx context.Context, key string) error
}

type cache struct {
	mounts map[string]*Mount
}

type Mount struct {
	Store
	Path string
}

func NewCache(ctx context.Context, mounts ...Mount) (Cache, error) {
	c := &cache{
		mounts: make(map[string]*Mount),
	}
	for _, mount := range mounts {
		path := trimPath(mount.Path)
		if path == "" || len(strings.Split(path, "/")) > 2 {
			return nil, fmt.Errorf("invalid mount path: %q", mount.Path)
		}
		c.mounts[path] = &mount
	}

	go c.gcLoop(ctx)

	return c, nil
}

func (c *cache) gcLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			gcCtx, cancel := context.WithTimeout(ctx, time.Second*30)
			c.collectGarbage(gcCtx)
			cancel()
		}
	}
}

func (c *cache) collectGarbage(ctx context.Context) {
	for _, mount := range c.mounts {
		c.cleanMount(ctx, mount)
	}
}

func (c *cache) cleanMount(ctx context.Context, s *Mount) {
	log := logger.From(ctx).WithFields(logger.Fields{
		"mount": s.Path,
	})

	unlock, ok := s.Lock(ctx)
	if !ok {
		return
	}
	defer unlock()

	records, err := s.List(ctx, "")
	if err != nil {
		//  TODO(ppacher): we should log this one ...
		log.V(4).Logf("failed to list records: %s", err)
		return
	}

	deleted := 0
	failed := 0
	for _, record := range records {
		if record.Record.IsValid() {
			continue
		}

		if err := s.Delete(ctx, record.Key); err != nil {
			failed++
			log.V(4).Logf("failed to delete key %s: %s", record.Key, err)
			continue
		}
		deleted++
	}

	log.WithFields(logger.Fields{
		"deleted": deleted,
		"failed":  failed,
	}).V(6).Logf("finished garbage collection")
}

func (c *cache) Write(ctx context.Context, key string, data []byte, opts ...Option) error {
	r := &Record{
		Data: data,
		Metadata: Metadata{
			CreatedAt: time.Now(),
		},
	}
	for _, opt := range opts {
		if err := opt(r); err != nil {
			return err
		}
	}
	// don't even store the entry if it's not valid already
	if !r.Metadata.IsValid() {
		return nil
	}
	keyRecord := KeyRecord{
		Key:    key,
		Record: r,
	}

	mount, err := c.findMount(key)
	if err != nil {
		return err
	}

	unlock, ok := mount.Lock(ctx)
	if !ok {
		return fmt.Errorf("failed to lock storage")
	}
	defer unlock()

	return mount.Put(ctx, keyRecord)
}

func (c *cache) Read(ctx context.Context, key string) ([]byte, *Metadata, error) {
	mount, err := c.findMount(key)
	if err != nil {
		return nil, nil, err
	}

	unlock, ok := mount.Lock(ctx)
	if !ok {
		return nil, nil, fmt.Errorf("failed to lock storage")
	}
	defer unlock()

	keyRecord, err := mount.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	md := keyRecord.Record.Metadata

	// this record is not valid anymore so we don't return it.
	if !md.IsValid() {
		return nil, nil, nil
	}

	// immediately delete the cache record if burn-after-reading
	// is enabled.
	// TODO(ppacher): should we only mark the record and use the gcLoop for
	//				  deletion?
	if md.BurnAfterReading {
		if err := mount.Delete(ctx, key); err != nil {
			// TODO(ppacher): how should we handle this error?
			return keyRecord.Record.Data, &md, err
		}
	}
	return keyRecord.Record.Data, &md, err
}

func (c *cache) Delete(ctx context.Context, key string) error {
	mount, err := c.findMount(key)
	if err != nil {
		return err
	}

	unlock, ok := mount.Lock(ctx)
	if !ok {
		return fmt.Errorf("failed to lock storage")
	}
	defer unlock()

	return mount.Delete(ctx, key)
}

func (c *cache) findMount(path string) (*Mount, error) {
	path = trimPath(path)
	for mount, store := range c.mounts {
		if strings.HasPrefix(path, mount) {
			return store, nil
		}
	}
	return nil, fmt.Errorf("key %s: no storage mounted", path)
}

func cloneRecord(r *Record) Record {
	c := new(Record)
	c.Metadata = r.Metadata
	if r.Metadata.NotValidAfter != nil {
		var v time.Time = *r.Metadata.NotValidAfter
		c.Metadata.NotValidAfter = &v
	}
	c.Data = make([]byte, len(r.Data))
	copy(c.Data, r.Data)
	return *c
}

func trimPath(path string) string {
	return strings.TrimSuffix(
		strings.TrimPrefix(
			path,
			"/",
		),
		"/",
	)
}
