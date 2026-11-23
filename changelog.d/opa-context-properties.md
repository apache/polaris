### New Features

- OPA: operators can set static key/value properties with
  `polaris.authorization.opa.context-properties.*`. Polaris sends them to OPA under
  `input.context.properties`, so a policy shared by several Polaris deployments can tell them
  apart.
