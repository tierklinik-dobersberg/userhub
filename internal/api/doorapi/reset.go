package doorapi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/cis/internal/app"
)

// ResetDoorEndpoint resets the door controller and the door itself
// and re-applies the current expected state.
func ResetDoorEndpoint(grp *app.Router) {
	grp.POST(
		"v1/reset",
		func(ctx context.Context, app *app.App, c *gin.Context) error {
			err := app.Door.Reset(ctx)
			if err != nil {
				return err
			}

			current, until := app.Door.Current(ctx)
			c.JSON(http.StatusOK, gin.H{
				"state": current,
				"until": until,
			})
			return nil
		},
	)
}
