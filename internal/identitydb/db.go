package identitydb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ppacher/system-conf/conf"
	"github.com/tierklinik-dobersberg/logger"
	"github.com/tierklinik-dobersberg/userhub/internal/loader"
	"github.com/tierklinik-dobersberg/userhub/internal/passwd"
	"github.com/tierklinik-dobersberg/userhub/pkg/models/v1alpha"
)

var (
	// ErrNotFound is returned when the requested user or group does not
	// exist.
	ErrNotFound = errors.New("not found")
	// ErrInvalidSectionCount indicates that eigher to much or to less sections
	// of a specific kind are found in file.
	ErrInvalidSectionCount = errors.New("unexpected number of sections")
)

type user struct {
	v1alpha.User

	passwordHash string
	passwordAlgo string
}

type group struct {
	v1alpha.Group
}

// Database describes the interface exposed by the identity database.
type Database interface {
	// Authenticate tries to authenticate a user. It returns true if the user/
	// password is correct. False otherwise.
	Authenticate(ctx context.Context, name string, password string) bool

	// GetUser returns the user object for the user identified by
	// it's name.
	GetUser(ctx context.Context, name string) (v1alpha.User, error)

	// GetGroup returns the group object for the groub identified by
	// it's name.
	GetGroup(ctx context.Context, name string) (v1alpha.Group, error)
}

// The actual in-memory implementation for identDB.
type identDB struct {
	ldr    *loader.Loader
	rw     sync.RWMutex
	users  map[string]*user
	groups map[string]*group
}

// New returns a new database that uses ldr.
func New(ldr *loader.Loader) (Database, error) {
	db := &identDB{
		ldr: ldr,
	}

	if err := db.reload(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *identDB) Authenticate(ctx context.Context, name, password string) bool {
	db.rw.RLock()
	defer db.rw.RUnlock()

	u, ok := db.users[strings.ToLower(name)]
	if !ok {
		return false
	}

	match, err := passwd.Compare(u.passwordAlgo, u.passwordHash, password)
	if err != nil {
		logger.From(ctx).Errorf("failed to compare password for user %s: %s", name, err)
	}

	return match
}

func (db *identDB) GetUser(ctx context.Context, name string) (v1alpha.User, error) {
	db.rw.RLock()
	defer db.rw.RUnlock()

	u, ok := db.users[strings.ToLower(name)]
	if !ok {
		return v1alpha.User{}, ErrNotFound
	}

	return u.User, nil
}

func (db *identDB) GetGroup(ctx context.Context, name string) (v1alpha.Group, error) {
	db.rw.RLock()
	defer db.rw.RUnlock()

	g, ok := db.groups[strings.ToLower(name)]
	if !ok {
		return v1alpha.Group{}, ErrNotFound
	}

	return g.Group, nil
}

func (db *identDB) reload() error {
	db.rw.Lock()
	defer db.rw.Unlock()

	db.users = make(map[string]*user, len(db.users))
	db.groups = make(map[string]*group, len(db.groups))

	userFiles, err := db.ldr.LoadUsers()
	if err != nil {
		return err
	}

	groupsFiles, err := db.ldr.LoadGroups()
	if err != nil {
		return err
	}

	// build the user map
	for _, f := range userFiles {
		u, err := buildUser(f)
		if err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}

		db.users[strings.ToLower(u.Name)] = u
	}

	// build the group map
	for _, f := range groupsFiles {
		g, err := buildGroup(f)
		if err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}

		db.groups[strings.ToLower(g.Name)] = g
	}

	// check all user.MemberOf groups actually exist
	for _, u := range db.users {
		for _, grpName := range u.GroupNames {
			if _, ok := db.groups[strings.ToLower(grpName)]; !ok {
				return fmt.Errorf("%s: member of %s: %w", u.Name, grpName, ErrNotFound)
			}
		}
	}

	return nil
}

func buildUser(f *conf.File) (*user, error) {
	u := new(user)

	secs := f.GetAll("user")
	if len(secs) == 0 || len(secs) > 1 {
		return nil, ErrInvalidSectionCount
	}
	sec := secs[0]

	var err error
	u.Name, err = sec.GetString("Name")
	if err != nil {
		return nil, fmt.Errorf("user.Name: %w", err)
	}

	u.passwordAlgo, err = sec.GetString("PasswordAlgo")
	if err != nil {
		return nil, fmt.Errorf("user.PasswordAlgo: %w", err)
	}

	u.passwordHash, err = sec.GetString("PasswordHash")
	if err != nil {
		return nil, fmt.Errorf("user.PasswordHash: %w", err)
	}

	u.Fullname, err = sec.GetString("Fullname")
	if err != nil && !conf.IsNotSet(err) {
		return nil, fmt.Errorf("user.Fullname: %w", err)
	}

	u.Mail = sec.GetStringSlice("Mail")
	u.PhoneNumber = sec.GetStringSlice("PhoneNumber")
	u.GroupNames = sec.GetStringSlice("MemberOf")

	return u, nil
}

func buildGroup(f *conf.File) (*group, error) {
	g := new(group)

	secs := f.GetAll("Group")
	if len(secs) == 0 || len(secs) > 1 {
		return nil, ErrInvalidSectionCount
	}
	sec := secs[0]

	var err error
	g.Name, err = sec.GetString("Name")
	if err != nil {
		return nil, fmt.Errorf("group.Name: %w", err)
	}

	g.Description, err = sec.GetString("Description")
	if err != nil && !conf.IsNotSet(err) {
		return nil, fmt.Errorf("group.Description: %w", err)
	}

	return g, nil
}
