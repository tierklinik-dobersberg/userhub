package permission

import (
	"context"
	"regexp"
	"strings"

	"github.com/tierklinik-dobersberg/cis/internal/schema"
	"github.com/tierklinik-dobersberg/logger"
)

// Matcher is used to decide on permission requests.
type Matcher struct {
	resolver *Resolver
}

// NewMatcher returns a new permission matcher.
func NewMatcher(resolver *Resolver) *Matcher {
	return &Matcher{resolver}
}

// Decide decides on the permission request and returns wether or not the
// request is permitted or not. In case of an error, false and the error is
// returned.
func (match *Matcher) Decide(ctx context.Context, req *Request) (bool, error) {
	l := logger.From(ctx).WithFields(req.AsFields())

	permissions, err := match.resolver.ResolveUserPermissions(ctx, req.User)
	if err != nil {
		return false, err
	}

	for _, permSet := range permissions {
		var allowedDescr string
		isAllowed := false
		for _, perm := range permSet {
			if match.IsApplicable(ctx, req, &perm) {
				if strings.ToLower(perm.Effect) == "allow" {
					isAllowed = true
					allowedDescr = perm.Description
				} else {
					// default is deny and that's stronger than
					// allow so we can abort and return immediately.
					l.Infof("denied by %q", perm.Description)
					return false, nil
				}
			}
		}

		if isAllowed {
			l.Infof("allowed by %q", allowedDescr)
			return true, nil
		}
	}

	l.Infof("denied by default (no matching permissions)")
	return false, nil
}

// IsApplicable returns true if perm is applicable to be used for a
// decision on req.
func (match *Matcher) IsApplicable(ctx context.Context, req *Request, perm *schema.Permission) bool {
	if len(perm.Domains) > 0 && !MatchNeedle(ctx, req.Domain, perm.Domains) {
		return false
	}
	if len(perm.Resources) > 0 && !MatchNeedle(ctx, req.Resource, perm.Resources) {
		return false
	}
	return true
}

// MatchNeedle checks if needle matches any of the regular expressions in haystack.
func MatchNeedle(ctx context.Context, needle string, haystack []string) bool {
	// TODO(ppacher): add LRU cache
	for _, hay := range haystack {
		re, err := regexp.Compile(hay)
		if err != nil {
			logger.From(ctx).Errorf("failed to compile regexp %q: %s", hay, err)
			continue
		}

		if re.MatchString(needle) {
			return true
		}
	}

	return false
}
