package cache

import (
	"fmt"
	"time"
)

// Option can be applied during cache writes and alternate
// various storage options.
type Option func(r *Record) error

// WithTTL adds a time-to-live to a cache entry.
// The entry will be deleted automatically after the TTL
// expired. If the entry is retrieved after the the TTL is
// expired but before the garbage collector removed it a
// not-found error is returned to the caller.
func WithTTL(ttl time.Duration) Option {
	return WithEOL(time.Now().Add(ttl))
}

// WithEOL is like WithTTL but allows to specify a time
// instead of a duration. For more information please refer
// to the documentation of WithTTL.
func WithEOL(eol time.Time) Option {
	return func(r *Record) error {
		if r.NotValidAfter != nil {
			return fmt.Errorf("end-of-life already set on record")
		}
		r.NotValidAfter = &eol
		return nil
	}
}

// WithBurnAfterReading marks the cache entry as one-time read.
// Once the cache entry is read it will be automatically deleted
// from the backing store.
func WithBurnAfterReading() Option {
	return func(r *Record) error {
		r.BurnAfterReading = true
		return nil
	}
}
