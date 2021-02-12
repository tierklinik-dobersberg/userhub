package customerapi

import (
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/httperr"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
)

// GetByIDEndpoint returns a JSON version of the customer
// with the given :id.
//
// GET /api/v1/customer/:id
func GetByIDEndpoint(grp *app.Router) {
	grp.GET(
		"v1/:source/:id",
		permission.Set{
			ReadCustomerAction,
		},
		func(ctx context.Context, app *app.App, c *gin.Context) error {
			source := c.Param("source")

			if source != "vetinf" && source != "neumayr" {
				return httperr.BadRequest(nil, "unknown source")
			}

			id, err := strconv.ParseInt(c.Param("id"), 10, 64)
			if err != nil {
				return httperr.BadRequest(err, "invalid ID")
			}

			customer, err := app.Customers.CustomerByCID(ctx, source, int(id))
			if err != nil {
				return err
			}

			model := CustomerModel(ctx, customer)
			c.JSON(http.StatusOK, model)
			return nil
		},
	)
}
