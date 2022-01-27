package httpcond

import (
	"github.com/labstack/echo/v4"
)

// And is a simple condition that represents a boolean
// AND between Left and Right.
type And struct {
	conds []Condition
}

// NewAnd returns a new and condition.
func NewAnd(conds ...Condition) Condition {
	return &And{
		conds: conds,
	}
}

// Match implements Condition.
func (and *And) Match(c echo.Context) (bool, error) {
	for _, cond := range and.conds {
		ok, err := cond.Match(c)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

// Or is a simple condition that represents a boolean
// OR between Left and Right.
type Or struct {
	conds []Condition
}

// NewOr returns a new or condition.
func NewOr(conds ...Condition) Condition {
	return &Or{
		conds: conds,
	}
}

// Match implements Condition.
func (or *Or) Match(c echo.Context) (bool, error) {
	for _, cond := range or.conds {
		ok, err := cond.Match(c)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}

	return false, nil
}

// Not is a simple condition that represents a boolean
// NOT on What.
type Not struct {
	What Condition
}

// NewNot returns a new not condition.
func NewNot(c Condition) *Not {
	return &Not{
		What: c,
	}
}

// Match implements Condition.
func (not *Not) Match(c echo.Context) (bool, error) {
	ok, err := not.What.Match(c)
	if err != nil {
		return false, err
	}

	return !ok, nil
}
