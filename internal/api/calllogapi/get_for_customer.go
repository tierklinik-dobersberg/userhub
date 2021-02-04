package calllogapi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/httperr"
	"github.com/tierklinik-dobersberg/cis/pkg/models/calllog/v1alpha"
)

// ForCustomerEndpoint allows searching for all calls that have been
// recorded for a given customer.
func ForCustomerEndpoint(router *app.Router) {
	router.GET(
		"v1/customer/:source/:id",
		func(ctx context.Context, app *app.App, c *gin.Context) error {
			source := c.Param("source")
			id := c.Param("id")
			if source == "" || id == "" {
				return httperr.BadRequest(nil)
			}

			records, err := app.CallLogs.ForCustomer(ctx, source, id)
			if err != nil {
				return err
			}

			// make sure we send an empty array instead of null
			if records == nil {
				records = make([]v1alpha.CallLog, 0)
			}

			c.JSON(http.StatusOK, records)

			return nil
		},
	)
}
