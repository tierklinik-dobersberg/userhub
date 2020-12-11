package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/userhub/internal/app"
)

// ProfileEndpoint serves the user profile of the user
// currently logged in.
//
// GET /api/v1/profile
func ProfileEndpoint(grp gin.IRouter) {
	grp.GET(
		"v1/profile",
		app.RequireSession(),
		func(c *gin.Context) {
			appCtx := app.From(c)
			if appCtx == nil {
				return
			}

			userName := app.SessionUser(c)

			user, err := appCtx.DB.GetUser(c.Request.Context(), userName)
			if err != nil {
				c.AbortWithError(http.StatusInternalServerError, err)
				return
			}

			c.JSON(http.StatusOK, user.User)
		},
	)
}
