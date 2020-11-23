package loader

import (
	"path/filepath"

	"github.com/ppacher/system-conf/conf"
	"github.com/tierklinik-dobersberg/userhub/internal/schema"
)

// Loader loads user and group definitions from the file system.
type Loader struct {
	searchRoots []string
}

// New returns a new loader that uses the given paths
// as it's search roots.
func New(paths ...string) *Loader {
	return &Loader{
		searchRoots: paths,
	}
}

// LoadUsers loads all users specified inside the search roots.
func (ldr *Loader) LoadUsers() ([]*conf.File, error) {
	return ldr.loadFiles("users", ".user", map[string][]conf.OptionSpec{
		"user":       schema.UserSpec,
		"permission": schema.PermissionSpec,
	})
}

// LoadGroups loads all groups specified inside the search roots.
func (ldr *Loader) LoadGroups() ([]*conf.File, error) {
	return ldr.loadFiles("groups", ".group", map[string][]conf.OptionSpec{
		"group":      schema.GroupSpec,
		"permission": schema.PermissionSpec,
	})
}

func (ldr *Loader) loadFiles(dir, ext string, spec map[string][]conf.OptionSpec) ([]*conf.File, error) {
	names := make(map[string]struct{})
	files := make([]*conf.File, 0)

	searchPaths := make([]string, len(ldr.searchRoots))
	for idx, root := range ldr.searchRoots {
		searchPaths[len(ldr.searchRoots)-1-idx] = filepath.Join(root, dir)
	}

	// get a list of all files that match dir and ext.
	for _, path := range searchPaths {
		dirFiles, err := conf.ReadDir(path, ext, spec)
		if err != nil {
			return nil, err
		}

		for _, file := range dirFiles {
			name := filepath.Base(file.Path)
			if _, ok := names[name]; ok {
				continue
			}

			dropins, err := conf.LoadDropIns(filepath.Base(file.Path), searchPaths)
			if err != nil {
				return nil, err
			}

			if err := conf.ApplyDropIns(file, dropins, spec); err != nil {
				return nil, err
			}

			files = append(files, file)
			names[name] = struct{}{}
		}
	}

	return files, nil
}
