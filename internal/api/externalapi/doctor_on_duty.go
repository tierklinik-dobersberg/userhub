package externalapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/labstack/echo/v4"
	"github.com/nyaruka/phonenumbers"
	idmv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1"
	"github.com/tierklinik-dobersberg/cis/internal/app"
	"github.com/tierklinik-dobersberg/cis/internal/permission"
	"github.com/tierklinik-dobersberg/cis/pkg/httperr"
	"github.com/tierklinik-dobersberg/cis/pkg/models/external/v1alpha"
	"github.com/tierklinik-dobersberg/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// CurrentDoctorOnDutyEndpoint provides a specialized endpoint
// to receive information about the current doctor on duty
// an any backups.
func CurrentDoctorOnDutyEndpoint(grp *app.Router) {
	grp.GET(
		"v1/doctor-on-duty",
		permission.OneOf{
			ReadOnDutyAction,
		},
		func(ctx context.Context, app *app.App, c echo.Context) error {
			ctx, span := otel.Tracer("").Start(ctx, "getDoctorOnDutyEndpoint")
			defer span.End()

			dateTime := time.Now()
			if at := c.QueryParam("at"); at != "" {
				var err error
				dateTime, err = app.ParseTime(time.RFC3339, at)
				if err != nil {
					return httperr.InvalidParameter("at", err.Error())
				}
			}

			ignoreOverwrites := false
			if val := c.QueryParam("ignore-overwrite"); val != "" {
				b, err := strconv.ParseBool(val)
				if err != nil {
					return httperr.InvalidParameter("ignore-overwrite", err.Error())
				}
				ignoreOverwrites = b
			}

			response, err := getDoctorOnDuty(ctx, app, dateTime, ignoreOverwrites)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())

				return err
			}

			if blob, err := json.MarshalIndent(response, "", "  "); err == nil {
				span.SetAttributes(
					attribute.String("tkd.doctor_on_duty.result", string(blob)),
				)
			} else if err != nil {
				span.SetAttributes(attribute.String("tkd.doctor_on_duty.result", err.Error()))
			}

			return c.JSON(http.StatusOK, response)
		},
	)
}

func activeOverwrite(ctx context.Context, app *app.App, t time.Time, profileById map[string]*idmv1.Profile) *v1alpha.DoctorOnDutyResponse {
	ctx, span := otel.Tracer("").Start(ctx, "getActiveOverwrite")
	defer span.End()

	log := log.From(ctx)

	// first check if we have an active overwrite for today

	log.V(7).Logf("[doctor-on-duty] retrieving active overwrites for %s", t)
	overwrite, err := app.OnCallOverwrites.GetActiveOverwrite(ctx, t)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}

	if err != nil {
		log.Errorf("failed to find active overwrite for %s: %s", t, err)

		return nil
	}

	if overwrite.DisplayName == "" {
		overwrite.DisplayName = "Manual Overwrite"
	}

	log.WithFields(logger.Fields{
		"overwrite": overwrite,
	}).Infof("found active overwrite, using that instead")

	parsed, err := phonenumbers.Parse(overwrite.PhoneNumber, app.Config.Country)
	if err == nil {
		overwrite.PhoneNumber = strings.ReplaceAll(
			phonenumbers.Format(parsed, phonenumbers.NATIONAL),
			" ",
			"",
		)
	} else {
		var phone *idmv1.PhoneNumber
		profile, ok := profileById[overwrite.UserId]
		if !ok || overwrite.UserId == "" {
			log.Errorf("found invalid emergency duty overwrite for %s", t)

			return nil
		}

		if len(profile.PhoneNumbers) > 0 {
			phone = profile.User.PrimaryPhoneNumber
			if phone == nil {
				phone = profile.PhoneNumbers[0]
			}
		}

		displayName := profile.User.DisplayName
		if displayName == "" {
			displayName = profile.User.Username
		}

		number := ""
		if phone != nil {
			number = phone.Number
		}

		var props map[string]any
		if extrapb := profile.User.Extra; extrapb != nil {
			props = extrapb.AsMap()
		}

		return &v1alpha.DoctorOnDutyResponse{
			Doctors: []v1alpha.DoctorOnDuty{
				{
					FullName:   displayName,
					Phone:      number,
					UserId:     profile.User.Id,
					Properties: props,
				},
			},
			Until:       overwrite.To,
			IsOverwrite: true,
		}
	}

	return &v1alpha.DoctorOnDutyResponse{
		Doctors: []v1alpha.DoctorOnDuty{
			{
				FullName: overwrite.DisplayName,
				Phone:    overwrite.PhoneNumber,
				UserId:   overwrite.UserId,
			},
		},
		Until:       overwrite.To,
		IsOverwrite: true,
	}
}

// trunk-ignore(golangci-lint/cyclop)
func getDoctorOnDuty(ctx context.Context, app *app.App, dateTime time.Time, ignoreOverwrites bool) (*v1alpha.DoctorOnDutyResponse, error) {
	ctx, sp := otel.Tracer("").Start(ctx, "getDoctorOnDuty")
	defer sp.End()

	log := log.From(ctx)
	dateTime = dateTime.In(app.Location())

	// fetch all users so we can convert usernames to phone numbers,
	// ...
	response, err := app.IDM.UserServiceClient.ListUsers(ctx, connect.NewRequest(&idmv1.ListUsersRequest{
		FieldMask: &fieldmaskpb.FieldMask{
			Paths: []string{"users.avatar"},
		},
		ExcludeFields: true,
	}))
	if err != nil {
		return nil, httperr.InternalError().SetInternal(err)
	}

	// build a small lookup map by username.
	profileById := make(map[string]*idmv1.Profile, len(response.Msg.Users))
	for _, profile := range response.Msg.Users {
		profileById[profile.User.Id] = profile
	}

	if !ignoreOverwrites {
		// check if there's an active overwrite for t. In this case, just return
		// that one and we're done.
		if res := activeOverwrite(ctx, app, dateTime, profileById); res != nil {
			return res, nil
		}
	}

	if app.RosterdServer == "" {
		log.Error("No rosterd server address defined, returning")
		return nil, nil
	}

	dod := &v1alpha.DoctorOnDutyResponse{
		Doctors:     nil,
		IsOverwrite: false,
		RosterDate:  dateTime.Format("2006-01-02"),
		// TODO(ppacher): we can set Until here based on the returned shifts
	}

	staffList, err := queryRosterOnCall(ctx, dateTime, app.RosterdServer)
	if err != nil {
		return nil, err
	}

	for _, userId := range staffList {
		u, ok := profileById[userId]
		if !ok {
			log.Errorf("rosterd returned unknown user %q", userId)
			continue
		}

		var phone string
		if u.User.PrimaryPhoneNumber != nil {
			phone = u.User.PrimaryPhoneNumber.Number
		} else {
			if len(u.PhoneNumbers) > 0 {
				phone = u.PhoneNumbers[0].Number
			}
		}

		displayName := u.User.DisplayName
		if displayName == "" {
			displayName = u.User.Username
		}

		var props map[string]any
		if extrapb := u.User.Extra; extrapb != nil {
			props = extrapb.AsMap()
		}

		dod.Doctors = append(dod.Doctors, v1alpha.DoctorOnDuty{
			UserId:     u.User.Id,
			FullName:   displayName,
			Phone:      phone,
			Properties: props,
		})
	}

	return dod, nil
}

func queryRosterOnCall(ctx context.Context, dateTime time.Time, rosterdServer string) ([]string, error) {
	ctx, sp := otel.Tracer("").Start(ctx, "queryRosterOnCall")
	defer sp.End()

	log := log.From(ctx)

	/*
		auth, _, err := session.GenerateM2MToken("cis", time.Second*5, []string{"internal:m2m"})
		if err != nil {
			return nil, httperr.InternalError().SetInternal(err)
		}
	*/
	auth := "FIXME"

	query := make(url.Values)
	query.Set("time", dateTime.Format(time.RFC3339))
	query.Set("tags", "OnCall")

	u := fmt.Sprintf("%s/v1/roster/on-duty?%s", strings.TrimPrefix(rosterdServer, "/"), query.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, httperr.InternalError().SetInternal(err)
	}
	req.Header.Set("Authorization", "Bearer "+auth)

	log.Infof("sending on-duty query to rosterd at %s", rosterdServer)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, httperr.InternalError().SetInternal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Errorf("received unexpected status code %d from rosterd", res.StatusCode)
		return nil, httperr.InternalError().SetInternal(fmt.Errorf("unexpected status code %d from rosterd", res.StatusCode))
	}

	var response struct {
		Staff []string `json:"staff"`
	}

	log.Infof("received rosterd response, decoding ...")
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, httperr.InternalError().SetInternal(err)
	}

	return response.Staff, nil
}
