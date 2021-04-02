package vetinf

import (
	"context"
	"errors"

	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/database/customerdb"
	"github.com/tierklinik-dobersberg/cis/internal/importer"
	"github.com/tierklinik-dobersberg/logger"
)

func getCustomerImporter(app *app.App, exporter *Exporter) *importer.Instance {
	return &importer.Instance{
		ID:             "vetinf-customer:" + app.Config.VetInfDirectory,
		Schedule:       app.Config.VetInfImportSchedule,
		RunImmediately: true,
		Handler: importer.ImportFunc(func() error {
			ctx := context.Background()

			ch, _, err := exporter.ExportCustomers(ctx)
			if err != nil {
				return err
			}

			countNew := 0
			countUpdated := 0
			countUnchanged := 0
			countDeleted := 0
			skippedDeleted := 0

			for customer := range ch {
				existing, err := app.Customers.CustomerByCID(ctx, "vetinf", customer.CustomerID)

				switch {
				case errors.Is(err, customerdb.ErrNotFound) && !customer.Deleted:
					err = app.Customers.CreateCustomer(ctx, &customer.Customer)
					if err == nil {
						countNew++
					}

				case errors.Is(err, customerdb.ErrNotFound) && customer.Deleted:
					// TODO(ppacher): create the customer if we use shadow-delete
					skippedDeleted++

				case existing != nil && customer.Deleted:
					err = app.Customers.DeleteCustomer(ctx, existing.ID.Hex())
					if err == nil {
						countDeleted++
					}

				case existing != nil && existing.Hash() != customer.Hash():
					// TODO(ppacher): if we use "shadow-delete" we might need to update
					// as well.
					customer.ID = existing.ID
					err = app.Customers.UpdateCustomer(ctx, &customer.Customer)
					if err == nil {
						countUpdated++
					}

				case existing != nil:
					countUnchanged++
				}

				if err != nil {
					logger.Errorf(ctx, "failed to import customer %s: %s", customer.CustomerID, err)
				}
			}

			logger.From(ctx).WithFields(logger.Fields{
				"new":            countNew,
				"updated":        countUpdated,
				"unchanged":      countUnchanged,
				"deleted":        countDeleted,
				"skippedDeleted": skippedDeleted,
			}).Infof("Import finished")

			return nil
		}),
	}
}
