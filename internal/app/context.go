package app

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/cis/internal/identitydb"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
	"github.com/tierklinik-dobersberg/service/server"
)

type contextKey string

const appContextKey = contextKey("app:context")

// App holds dependencies for cis API request handlers.
type App struct {
	Config  *Config
	Matcher *permission.Matcher
	DB      identitydb.Database
}

func (app *App) String() string {
	return "app.App"
}

// NewApp context creates a new application context.
func NewApp(cfg *Config, matcher *permission.Matcher, db identitydb.Database) *App {
	return &App{
		Config:  cfg,
		Matcher: matcher,
		DB:      db,
	}
}

// With adds app to ctx.
func With(ctx context.Context, app *App) context.Context {
	return context.WithValue(ctx, appContextKey, app)
}

// ServerOption returns a server option that adds app to
// each request. Useful if used together with From() in
// request handlers.
func ServerOption(app *App) server.Option {
	return server.WithPreHandler(AddToRequest(app))
}

// AddToRequest returns a (service/server).PreHandlerFunc that
// adds app to each incoming HTTP request.
func AddToRequest(app *App) server.PreHandlerFunc {
	return func(req *http.Request) *http.Request {
		ctx := req.Context()

		return req.WithContext(
			With(ctx, app),
		)
	}
}

// From returns the App associated with c.
// If there is no context assigned to c the request
// is terminated with 500 Internal Server error.
func From(c *gin.Context) *App {
	val, _ := c.Request.Context().Value(appContextKey).(*App)

	if val == nil {
		server.AbortRequest(c, http.StatusInternalServerError, errors.New("No AppCtx available"))
		return nil
	}

	return val
}
