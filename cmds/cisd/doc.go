package main

import (
	"github.com/ppacher/system-conf/conf"
	"github.com/tierklinik-dobersberg/cis/internal/autodoc"
	"github.com/tierklinik-dobersberg/cis/internal/calendar"
	"github.com/tierklinik-dobersberg/cis/internal/cfgspec"
	"github.com/tierklinik-dobersberg/service/server"
	"github.com/tierklinik-dobersberg/service/svcenv"
)

var globalConfigFile = autodoc.MustRegister(autodoc.File{
	Name:        "cis.conf",
	Description: "The main configuration file for CIS.",
	LookupPaths: []string{
		svcenv.Env().ConfigurationDirectory,
	},
	Sections: conf.FileSpec{
		"Global": autodoc.MergeOptions(
			cfgspec.ConfigSpec,
			cfgspec.DatabaseSpec,
			cfgspec.IdentityConfigSpec,
			cfgspec.MqttSpec,
		),
		"Import":         cfgspec.VetInfSpec,
		"Listener":       server.ListenerSpec,
		"UserProperty":   cfgspec.UserSchemaExtension,
		"OpeningHour":    cfgspec.OpeningHoursSpec,
		"Integration":    cfgspec.IntegrationConfigSpec,
		"Voicemail":      cfgspec.VoiceMailSpec,
		"MongoLog":       cfgspec.MongoLogSpec,
		"GoogleCalendar": calendar.ConfigSpec,
		"CORS":           server.CORSSpec,
	},
})

var uiConfigFile = autodoc.MustRegister(autodoc.File{
	Name:        "ui.conf",
	Description: "Configuration file for the User-Interface.",
	LookupPaths: []string{
		svcenv.Env().ConfigurationDirectory,
	},
	Sections: conf.FileSpec{
		"UI":                   cfgspec.UISpec,
		"ExternalLink":         cfgspec.ExternalLinkSpec,
		"QuickRosterOverwrite": cfgspec.QuickRosterOverwriteSpec,
		"KnownPhoneExtension":  cfgspec.KnownPhoneExtensionSpec,
	},
})
