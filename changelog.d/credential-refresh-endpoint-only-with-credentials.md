### Changes
- The credential refresh endpoint (`*.refresh-credentials-endpoint` in the `config` of a
  `LoadTableResult`) is now included only when credentials were actually vended for the request.
  Storage configurations that return no credentials no longer advertise the endpoint.
