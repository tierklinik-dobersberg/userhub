package main

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/tierklinik-dobersberg/cis/internal/database/customerdb"
	"github.com/tierklinik-dobersberg/cis/internal/vetinf"
	"github.com/tierklinik-dobersberg/logger"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/crypto/ssh/terminal"
)

func getImportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import customer data from various sources",
	}

	cmd.AddCommand(
		getVetinfImportCommand(),
	)

	return cmd
}

func getVetinfImportCommand() *cobra.Command {
	var (
		showProgress bool
	)

	cmd := &cobra.Command{
		Use:   "vetinf",
		Short: "Import customer data from vetinf",
		Run: func(_ *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			app := getApp(ctx)

			exporter, err := vetinf.NewExporter(app.Config.VetInf)
			if err != nil {
				log.Fatal(err)
			}

			all, total, err := exporter.ExportCustomers(ctx)
			if err != nil {
				return
			}

			var (
				p   *mpb.Progress
				bar *mpb.Bar
			)

			if showProgress {
				p = mpb.New(mpb.WithWidth(64))
				bar = p.AddBar(int64(total),
					mpb.PrependDecorators(decor.CountersNoUnit("%d / %d")),
					mpb.AppendDecorators(decor.Percentage()),
					mpb.AppendDecorators(decor.Name(" ETA: ")),
					mpb.AppendDecorators(decor.AverageETA(decor.ET_STYLE_GO)),
				)
			}

			updated := 0
			created := 0

			for customer := range all {
				if bar != nil {
					bar.Increment()
				}

				existing, err := app.Customers.CustomerByCID(context.Background(), customer.CustomerID)
				if errors.Is(err, customerdb.ErrNotFound) {
					err = app.Customers.CreateCustomer(context.Background(), customer)
					if err == nil {
						created++
					}
				} else if existing != nil && existing.Hash() != customer.Hash() {
					customer.ID = existing.ID

					err = app.Customers.UpdateCustomer(context.Background(), customer)
					if err == nil {
						updated++
					}
				}

				if err != nil {
					logger.Errorf(ctx, "failed to query or create customer with cid %d: %s", customer.ID, err)
					os.Exit(1)
				}
			}

			if p != nil {
				p.Wait()
			}

			logger.DefaultLogger().WithFields(logger.Fields{
				"updated": updated,
				"created": created,
			}).Infof("operation finished successfully")
		},
	}

	flags := cmd.Flags()
	{
		flags.BoolVar(&showProgress, "progress", terminal.IsTerminal(1), "Show a progress bar while importing")
	}

	return cmd
}
