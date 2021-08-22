package cache

import "context"

type KeyRecord struct {
	Key    string
	Record *Record
}

// UnlockFunc is returned when locking a cache store.
type UnlockFunc func()

// Store is used to store cache entries. It should only care about
// the actual storage of records but about record metadata, garbage
// collection or EOL handling. Store should not consider record data
// and act as if they are opaque data.
type Store interface {
	// Lock should lock the storage and return a method
	// to unlock.
	Lock(ctx context.Context) (UnlockFunc, bool)

	// Put stores r under key.
	Put(ctx context.Context, r KeyRecord) error

	// Get returns the record stored under key. It should
	// not care about records TTLs or other records metadata.
	Get(ctx context.Context, key string) (*KeyRecord, error)

	// List should list all records that start with prefix. If a prefix
	// search is not supported by the underlying storage List can also
	// return all records. The caller of List must expect to receive records
	// with keys that are not prefixed with prefix.
	List(ctx context.Context, prefix string) ([]*KeyRecord, error)

	// Delete should returns any record stored under key.
	Delete(ctx context.Context, key string) error
}
