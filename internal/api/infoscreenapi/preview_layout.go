package infoscreenapi

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/infoscreen/layouts"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
	"github.com/tierklinik-dobersberg/cis/internal/utils"
	"github.com/tierklinik-dobersberg/cis/pkg/httperr"
	"github.com/tierklinik-dobersberg/cis/pkg/models/infoscreen/v1alpha"
	"github.com/tierklinik-dobersberg/cis/pkg/multierr"
	"github.com/tierklinik-dobersberg/cis/runtime/session"
)

func RenderLayoutPreviewEndpoint(router *app.Router) {
	router.POST(
		"v1/preview",
		permission.OneOf{
			ActionLayoutPreview,
		},
		func(ctx context.Context, app *app.App, c *gin.Context) error {
			sess := session.Get(c)
			if sess == nil {
				return httperr.InternalError(nil, "missing session")
			}

			var slide v1alpha.Slide
			if err := json.NewDecoder(c.Request.Body).Decode(&slide); err != nil {
				return err
			}
			key, err := utils.Nonce(32)
			if err != nil {
				return err
			}
			if err := sess.SetEphemeral(key, slide, time.Minute); err != nil {
				return err
			}
			c.JSON(http.StatusOK, gin.H{
				"key": key,
			})
			return nil
		},
	)

	router.GET(
		"v1/preview/:key/*resource",
		permission.OneOf{
			ActionLayoutPreview,
		},
		func(ctx context.Context, app *app.App, c *gin.Context) error {
			resource := strings.TrimPrefix(c.Param("resource"), "/")
			theme := c.Query("theme")
			if theme == "" {
				theme = "white"
			}

			key := c.Param("key")
			sess := session.Get(c)
			if sess == nil {
				return httperr.InternalError(nil, "missing session")
			}

			data, ttl, err := sess.GetEphemeral(key)
			if err != nil {
				return err
			}
			if ttl.IsZero() {
				return httperr.NotFound("slide-preview", key, nil)
			}
			s, ok := data.(v1alpha.Slide)
			if !ok {
				return httperr.InternalError(nil, "invalid data type for ephemeral key")
			}

			l, err := app.LayoutStore.Get(ctx, s.Layout)
			if err != nil {
				return err
			}

			if strings.HasPrefix(resource, "uploaded/") {
				http.ServeFile(c.Writer, c.Request, filepath.Join(
					app.Config.InfoScreenConfig.UploadDataDirectory,
					strings.TrimPrefix(resource, "uploaded/"),
				))
				return nil
			}

			content, err := layouts.Render(l, s.Vars)
			if err != nil {
				return err
			}

			return renderPlayer(&PlayContext{
				ShowName: "preview",
				Embedded: true,
				Theme:    theme,
				Slides: []PlaySlide{
					{
						Content:     template.HTML(content),
						AutoAnimate: s.AutoAnimate,
						Background:  s.Background,
					},
				},
			}, c.Writer)
		},
	)
}

func parseLayoutVars(l *layouts.Layout, query url.Values) (layouts.Vars, error) {
	errors := new(multierr.Error)

	vars := layouts.Vars{}
	for key, values := range query {
		def := l.Var(key)
		if def == nil {
			// we do allow unknown query parameters as they might control
			// different behavior and are not meant as layout-vars
			continue
		}

		if len(values) != 1 && def.Type != layouts.TypeStringList {
			errors.Addf("query parameter %s only allowed once", key)
			continue
		}

		var (
			val interface{}
			err error
		)
		switch def.Type {
		case layouts.TypeBool:
			val, err = strconv.ParseBool(values[0])
		case layouts.TypeString:
			val, _ = url.QueryUnescape(values[0])
		case layouts.TypeStringList:
			val = values
		case layouts.TypeNumber:
			val, err = strconv.ParseFloat(values[0], 64)
		case layouts.TypeImage, layouts.TypeVideo:
			val = values[0]
		default:
			err = fmt.Errorf("unsupported variable type %s", def.Type)
		}
		if err != nil {
			errors.Add(err)
			continue
		}

		vars[key] = val
	}
	return vars, errors.ToError()
}
