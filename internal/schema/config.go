package schema

import "github.com/ppacher/system-conf/conf"

// GlobalConfigSpec defines the available configuration values for the
// [Global] configuration section.
var GlobalConfigSpec = []conf.OptionSpec{
	{
		Name:        "Secret",
		Description: "Secret used to sign session cookies. If empty, a temporary secret is created.",
		Type:        conf.StringType,
	},
	{
		Name:        "CookieName",
		Default:     "userhub",
		Description: "Name of the session cookie",
		Type:        conf.StringType,
	},
	{
		Name:        "CookieDomain",
		Required:    true,
		Description: "The domain for which session cookies should be created",
		Type:        conf.StringType,
	},
	{
		Name:        "InsecureCookies",
		Default:     "no",
		Description: "Wether or not the session cookie should be HTTPS only",
		Type:        conf.BoolType,
	},
	{
		Name:        "AccessLogFile",
		Description: "Path to access lo file",
		Type:        conf.StringType,
	},
}

// ListenerSpec defines the available configuration values for the
// [Listener] sections.
var ListenerSpec = []conf.OptionSpec{
	{
		Name:        "Address",
		Required:    true,
		Description: "Address to listen on in the format of <ip/hostname>:<port>.",
		Type:        conf.StringType,
	},
	{
		Name:        "CertificateFile",
		Description: "Path to the TLS certificate file (PEM format)",
		Type:        conf.StringType,
	},
	{
		Name:        "PrivateKeyFile",
		Description: "Path to the TLS private key file (PEM format)",
		Type:        conf.StringType,
	},
}
