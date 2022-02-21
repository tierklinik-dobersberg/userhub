package triggerapi

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
	"github.com/tierklinik-dobersberg/cis/pkg/httperr"
	"github.com/tierklinik-dobersberg/cis/runtime/event"
	"github.com/tierklinik-dobersberg/cis/runtime/session"
	"github.com/tierklinik-dobersberg/cis/runtime/trigger"
	"github.com/tierklinik-dobersberg/logger"
)

func ExecuteTriggerGroupEndpoint(router *app.Router) {
	router.POST(
		"v1/group/:groupName",
		permission.Anyone, // we'l verify that ourself
		func(ctx context.Context, app *app.App, c echo.Context) error {
			sess := session.Get(c)
			groupName := c.Param(("groupName"))

			instances, err := findAllowedGroupMembers(
				ctx,
				sess.User.Name,
				sess.ExtraRoles(),
				app,
				trigger.DefaultRegistry.Instances(),
				groupName,
			)
			if err != nil {
				return err
			}

			result := make([]TriggerInstance, 0, len(instances))
			for _, instance := range instances {
				if !instance.Wants(externalTriggerID) {
					continue
				}
				err := instance.Handle(ctx, &event.Event{
					ID:      "__external",
					Created: time.Now(),
				})
				if err != nil {
					logger.From(ctx).Errorf("failed to handle external trigger: %s: %s", instance.Name(), err.Error())
				}

				result = append(result, TriggerInstance{
					Name:        instance.Name(),
					Description: instance.Description(),
					Pending:     instance.Pending(),
					Groups:      instance.Groups(),
				})
			}
			if len(result) == 0 {
				return httperr.PreconditionFailed("no instance in this group supports being triggered via API")
			}

			return c.JSON(http.StatusAccepted, TriggerListResponse{
				Instances: result,
			})
		},
	)
}

func isInSlice(needle string, haystack []string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}

	return false
}

func findAllowedGroupMembers(ctx context.Context, username string, extraRoles []string, app *app.App, triggers []*trigger.Instance, groupName string) ([]*trigger.Instance, error) {
	var (
		result     []*trigger.Instance
		foundGroup bool
	)
	for _, instance := range triggers {
		if !isInSlice(groupName, instance.Groups()) {
			continue
		}
		foundGroup = true
		req := &permission.Request{
			User:     username,
			Action:   ExecuteTriggerAction.Name,
			Resource: instance.Name(),
		}
		permitted, err := app.Matcher.Decide(ctx, req, extraRoles)
		if err != nil {
			return nil, err
		}
		if permitted {
			result = append(result, instance)
		}
	}
	if !foundGroup {
		return nil, httperr.NotFound("trigger group", groupName)
	}
	if len(result) == 0 {
		return nil, httperr.Forbidden("permission denied")
	}

	return result, nil
}
