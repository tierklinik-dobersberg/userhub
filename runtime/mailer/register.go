package mailer

import (
	"github.com/tierklinik-dobersberg/cis/runtime"
	"github.com/tierklinik-dobersberg/cis/runtime/trigger"
)

var (
	ConfigBuilder = runtime.NewConfigSchemaBuilder(addToSchema)
	AddToSchema   = ConfigBuilder.AddToSchema
)

func addToSchema(schema *runtime.ConfigSchema) error {
	return schema.Register(runtime.Schema{
		Name:        "Mailer",
		Description: "Configure a SMTP server to allow sending emails.",
		Spec:        AccountSpec,
		Multi:       false,
	})
}

func init() {
	// create a global [Mailer] configuration section
	runtime.Must(
		AddToSchema(runtime.GlobalSchema),
	)
	// Register the mailer as a trigger type at the default
	// registry as [SendMail]
	runtime.Must(
		AddTriggerType("SendMail", trigger.DefaultRegistry),
	)
}
