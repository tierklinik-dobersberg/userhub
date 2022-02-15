package configapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
	"github.com/tierklinik-dobersberg/cis/runtime"
)

type DeleteConfigResponse struct {
	ID      string `json:"id"`
	Warning string `json:"warning,omitempty"`
}

func DeleteConfigEndpoint(r *app.Router) {
	r.DELETE(
		"v1/schema/:key/:id",
		permission.OneOf{ConfigManagementAction},
		func(ctx context.Context, app *app.App, c echo.Context) error {
			id := c.Param("id")

			var warning string
			if err := runtime.GlobalSchema.Delete(ctx, id); err != nil {
				var notifyErr *runtime.NotificationError
				if errors.As(err, &notifyErr) {
					warning = notifyErr.Wrapped.Error()
				} else {
					return err
				}
			}

			c.JSON(http.StatusOK, DeleteConfigResponse{
				ID:      id,
				Warning: warning,
			})
			return nil
		},
	)
}
