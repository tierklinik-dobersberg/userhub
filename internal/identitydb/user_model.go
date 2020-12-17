package identitydb

import (
	"fmt"
	"strings"

	"github.com/ppacher/system-conf/conf"
	"github.com/tierklinik-dobersberg/cis/internal/schema"
	"github.com/tierklinik-dobersberg/cis/internal/utils"
)

type user struct {
	schema.User `section:"User"`

	Permissions []*schema.Permission `section:"Permission"`
}

func (db *identDB) loadUsers(identityDir string) error {
	userFiles, err := utils.LoadFiles(identityDir, ".user", conf.FileSpec{
		"User":       append(schema.UserSpec, db.userPropertySpecs...),
		"Permission": schema.PermissionSpec,
	})
	if err != nil {
		return err
	}

	// build the user map
	for _, f := range userFiles {
		u, err := buildUser(f, db.userPropertySpecs)
		if err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}

		db.users[strings.ToLower(u.Name)] = u
	}

	return nil
}

func buildUser(f *conf.File, userPropertySpecs []conf.OptionSpec) (*user, error) {
	spec := conf.FileSpec{
		"User":       schema.UserSpec,
		"Permission": schema.PermissionSpec,
	}

	var u user
	if err := spec.Decode(f, &u); err != nil {
		return nil, err
	}

	// Build custom user properties
	// We do not perform any validation here as sec.Options
	// is expected to have been validated already using Validate()
	// and Prepare.
	sec := f.Get("User") // there can only be one User section

	if len(userPropertySpecs) > 0 {
		u.Properties = make(map[string]interface{})
		for _, spec := range userPropertySpecs {
			u.Properties[spec.Name] = sec.GetAs(spec.Name, spec.Type)
		}
	}

	return &u, nil
}
