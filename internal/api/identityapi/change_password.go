package identityapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/httperr"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
	"github.com/tierklinik-dobersberg/cis/internal/session"
)

// ChangePasswordEndpoint allows a user to change it's own password.
func ChangePasswordEndpoint(grp *app.Router) {
	grp.PUT(
		"v1/profile/password",
		permission.Anyone,
		func(ctx context.Context, app *app.App, c *gin.Context) error {
			body := struct {
				Current     string `json:"current"`
				NewPassword string `json:"newPassword"`
			}{}

			if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
				return httperr.BadRequest(err, "invalid body")
			}

			if body.Current == "" {
				return httperr.BadRequest(nil, "missing current password")
			}

			if body.NewPassword == "" {
				return httperr.BadRequest(nil, "missing new password")
			}

			sess := session.Get(c)
			if !app.Identities.Authenticate(ctx, sess.User.Name, body.Current) {
				return httperr.BadRequest(nil)
			}

			if err := app.Identities.SetUserPassword(ctx, sess.User.Name, body.NewPassword, "bcrypt"); err != nil {
				return err
			}

			c.Status(http.StatusNoContent)
			return nil
		},
	)
}
