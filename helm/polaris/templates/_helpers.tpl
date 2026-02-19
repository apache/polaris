{{/*
  Copyright (C) 2024 Dremio

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/}}

{{/*
  Expand the name of the chart.
*/}}
{{- define "polaris.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
  Create a default fully qualified app name.
  We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
  If release name contains chart name it will be used as a full name.
*/}}
{{- define "polaris.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
  Create a default fully qualified app name, with a custom suffix. Useful when the name will
  have a suffix appended to it, such as for the management service name.
*/}}
{{- define "polaris.fullnameWithSuffix" -}}
{{- $global := index . 0 }}
{{- $suffix := index . 1 }}
{{- if not (hasPrefix "-" $suffix) }}
{{- $suffix = printf "-%s" $suffix }}
{{- end }}
{{- $length := int (sub 63 (len $suffix)) }}
{{- if $global.Values.fullnameOverride }}
{{- $global.Values.fullnameOverride | trunc $length }}{{ $suffix }}
{{- else }}
{{- $name := default $global.Chart.Name $global.Values.nameOverride }}
{{- if contains $name $global.Release.Name }}
{{- $global.Release.Name | trunc $length }}{{ $suffix }}
{{- else }}
{{- printf "%s-%s" $global.Release.Name $name | trunc $length }}{{ $suffix }}
{{- end }}
{{- end }}
{{- end }}

{{/*
  Create chart name and version as used by the chart label.
*/}}
{{- define "polaris.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
  Common labels
*/}}
{{- define "polaris.labels" -}}
helm.sh/chart: {{ include "polaris.chart" . }}
{{ include "polaris.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
  Selector labels
*/}}
{{- define "polaris.selectorLabels" -}}
app.kubernetes.io/name: {{ include "polaris.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Validate that only one of ingress or httproute is enabled
*/}}
{{- define "polaris.validateRouting" -}}
{{- if and .Values.ingress.enabled .Values.httproute.enabled }}
{{- fail "Cannot enable both ingress and httproute. Please enable only one." }}
{{- end }}
{{- if and (not .Values.httproute.enabled) .Values.gateway.enabled }}
{{- fail "In order to use the gateway please enable the httproute and disable the ingress."}}
{{- end }}
{{- end }}

{{/*
  Create the name of the service account to use
*/}}
{{- define "polaris.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "polaris.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Merges a configuration tree into the destination configuration map. See configmap.yaml template.
Two styles of configuration trees are supported:
- Flattened configuration tree: The configuration option names are specified as a dot-separated
  string, and the configuration option values are the values of the configuration options. E.g.:
  "key1.subkey1": "value1"
  "key1.subkey2.subsubkey1": "value2"
- Nested configuration tree: The configuration option names are specified as a nested structure.
  The resulting option names are formed by concatenating the nested keys with a dot separator.
  E.g.:
  key1:
    subkey1: "value1"
    subkey2:
      subsubkey1: "value2"
The configuration option values are evaluated as templates against the global context before being
printed.
*/}}
{{- define "polaris.mergeConfigTree" -}}
{{- $advConfig := index . 0 -}}
{{- $prefix := index . 1 -}}
{{- $dest := index . 2 -}}
{{- range $key, $val := $advConfig -}}
{{- $name := ternary $key (print $prefix "." $key) (eq $prefix "") -}}
{{- if kindOf $val | eq "map" -}}
{{- list $val $name $dest | include "polaris.mergeConfigTree" -}}
{{- else -}}
{{- $_ := set $dest $name $val -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Prints the configuration option to the destination configmap entry. See configmap.yaml template.
Any nil values will be printed as empty config options; otherwise, the value will be evaluated
as a template against the global context, then printed. Furthermore, if the value contains
line breaks, they will be escaped and a multi-line option will be printed.
*/}}
{{- define "polaris.appendConfigOption" -}}
{{- $key := index . 0 -}}
{{- $value := index . 1 -}}
{{- $global := index . 2 -}}
{{- $valAsString := "" -}}
{{/* Note: We really need the statement below to be "if ne $value nil". This is unusual, but here we
need to distinguish other zero-values from nil. For example, "someProperty: false" or
"someProperty: 0" should result in the config property "someProperty" being included in the
ConfigMap, with value false or 0.*/}}
{{- if ne $value nil -}}
{{- $valAsString = tpl (toString $value) $global -}}
{{- if contains "\r\n" $valAsString -}}
{{- $valAsString = $valAsString | nindent 4 | replace "\r\n" "\\\r\n" -}}
{{- else if contains "\n" $valAsString -}}
{{- $valAsString = $valAsString | nindent 4 | replace "\n" "\\\n" -}}
{{- end -}}
{{- end -}}
{{ print (include "polaris.escapeConfigOptionKey" $key) "=" $valAsString }}
{{- end -}}

{{/*
Escapes a property key to be used in a configmap, conforming with the Java parsisng rules for
property files: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Properties.html#load(java.io.Reader)
- Escapes all backslashes.
- Escapes all key termination characters: '=', ':' and whitespace.
*/}}
{{- define "polaris.escapeConfigOptionKey" -}}
{{- $key := . -}}
{{- $key | replace "\\" "\\\\" | replace "=" "\\=" | replace ":" "\\:" | replace " " "\\ " -}}
{{- end -}}

{{/*
Prints the config volume definition for deployments and jobs.
*/}}
{{- define "polaris.configVolume" -}}
- name: config-volume
  projected:
    sources:
      - configMap:
          name: {{ include "polaris.fullname" . }}
          items:
            - key: application.properties
              path: application.properties
      {{- include "polaris.configVolumeAuthenticationOptions" (list "" .Values.authentication .) | nindent 6 }}
      {{- range $realm, $auth := .Values.authentication.realmOverrides -}}
      {{- include "polaris.configVolumeAuthenticationOptions" (list $realm $auth $) | nindent 6 }}
      {{- end -}}
{{- end -}}

{{/*
Prints an environment variable for a secret key reference.
*/}}
{{- define "polaris.secretToEnv" -}}
{{- $secret := index . 0 -}}
{{- $keyRef := index . 1 -}}
{{- $varName := index . 2 -}}
{{- $key := get $secret $keyRef -}}
{{- if and $secret.name $key }}
- name: {{ $varName }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret.name }}
      key: {{ $key }}
{{- end -}}
{{- end -}}

{{/*
Converts a Kubernetes quantity to a number (int64 if possible or float64 otherwise).
It handles raw numbers as well as quantities with suffixes
like m, k, M, G, T, P, E, ki, Mi, Gi, Ti, Pi, Ei.
It also handles scientific notation.
Quantities should be positive, so negative values, zero, or any unparsable number
will result in a failure.
https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/
*/}}
{{- define "polaris.quantity" -}}
{{- $quantity := . -}}
{{- $n := $quantity | float64 -}}
{{- if kindIs "string" $quantity -}}
{{- if hasSuffix "m" $quantity -}}
{{- $n = divf (trimSuffix "m" $quantity | float64) 1000.0 -}}
{{- else if hasSuffix "k" $quantity -}}
{{- $n = trimSuffix "k" $quantity | int64 | mul 1000 -}}
{{- else if hasSuffix "M" $quantity -}}
{{- $n = trimSuffix "M" $quantity | int64 | mul 1000000 -}}
{{- else if hasSuffix "G" $quantity -}}
{{- $n = trimSuffix "G" $quantity | int64 | mul 1000000000 -}}
{{- else if hasSuffix "T" $quantity -}}
{{- $n = trimSuffix "T" $quantity | int64 | mul 1000000000000 -}}
{{- else if hasSuffix "P" $quantity -}}
{{- $n = trimSuffix "P" $quantity | int64 | mul 1000000000000000 -}}
{{- else if hasSuffix "E" $quantity -}}
{{- $n = trimSuffix "E" $quantity | int64 | mul 1000000000000000000 -}}
{{- else if hasSuffix "ki" $quantity -}}
{{- $n = trimSuffix "ki" $quantity | int64 | mul 1024 -}}
{{- else if hasSuffix "Mi" $quantity -}}
{{- $n = trimSuffix "Mi" $quantity | int64 | mul 1048576 -}}
{{- else if hasSuffix "Gi" $quantity -}}
{{- $n = trimSuffix "Gi" $quantity | int64 | mul 1073741824 -}}
{{- else if hasSuffix "Ti" $quantity -}}
{{- $n = trimSuffix "Ti" $quantity | int64 | mul 1099511627776 -}}
{{- else if hasSuffix "Pi" $quantity -}}
{{- $n = trimSuffix "Pi" $quantity | int64 | mul 1125899906842624 -}}
{{- else if hasSuffix "Ei" $quantity -}}
{{- $n = trimSuffix "Ei" $quantity | int64 | mul 1152921504606846976 -}}
{{- end -}}
{{- end -}}
{{- if le ($n | float64) 0.0 -}}
{{- fail (print "invalid quantity: " $quantity) -}}
{{- end -}}
{{- if kindIs "float64" $n -}}
{{- printf "%f" $n -}}
{{- else -}}
{{- printf "%v" $n -}}
{{- end -}}
{{- end -}}

{{/*
Helper template to validate and register a container port.
Arguments (passed as a list):
  0: $ports dict (mutated) - maps port number to {name, protocol}
  1: $names dict (mutated) - maps port name to port number
  2: $errorPrefix string - prefix for error messages (e.g. "service.ports[0]")
  3: $port object - the port definition from the values.yaml with name, port, targetPort, protocol
*/}}
{{- define "polaris.validateContainerPort" -}}
{{- $ports := index . 0 -}}
{{- $names := index . 1 -}}
{{- $errorPrefix := index . 2 -}}
{{- $port := index . 3 -}}
{{- $portNumber := coalesce $port.targetPort $port.port | toString -}}
{{- $protocol := $port.protocol | default "TCP" -}}
{{- if hasKey $ports $portNumber -}}
{{- $existing := get $ports $portNumber -}}
{{- if ne $port.name (index $existing "name") -}}
{{- fail (printf "%s: port number %s has conflicting name, expected %v, got %v" $errorPrefix $portNumber (index $existing "name") $port.name) -}}
{{- end -}}
{{- if ne $protocol (index $existing "protocol") -}}
{{- fail (printf "%s: port number %s has conflicting protocol, expected %v, got %v" $errorPrefix $portNumber (index $existing "protocol") $protocol) -}}
{{- end -}}
{{- else if hasKey $names $port.name -}}
{{- fail (printf "%s: port name %s has conflicting number, expected %v, got %v" $errorPrefix $port.name (get $names $port.name) $portNumber) -}}
{{- else -}}
{{- $_ := set $ports $portNumber (dict "name" $port.name "protocol" $protocol) -}}
{{- $_ = set $names $port.name $portNumber -}}
{{- end -}}
{{- end -}}

{{/*
Prints the ports section of the container spec. Iterates over all service port declarations
and determines which ports the container should expose, ensuring no duplicate port numbers
or port names.
*/}}
{{- define "polaris.containerPorts" -}}
{{- $ports := dict -}}
{{- $names := dict -}}
{{- /* Main service ports */ -}}
{{- range $i, $port := .Values.service.ports -}}
{{- include "polaris.validateContainerPort" (list $ports $names (printf "service.ports[%d]" $i) $port) -}}
{{- end -}}
{{- /* Management service ports */ -}}
{{- range $i, $port := .Values.managementService.ports -}}
{{- include "polaris.validateContainerPort" (list $ports $names (printf "managementService.ports[%d]" $i) $port) -}}
{{- end -}}
{{- /* Extra service ports */ -}}
{{- range $i, $svc := .Values.extraServices -}}
{{- if $svc.nameSuffix -}}
{{- range $j, $port := $svc.ports -}}
{{- include "polaris.validateContainerPort" (list $ports $names (printf "extraServices[%d].ports[%d]" $i $j) $port) -}}
{{- end -}}
{{- end -}}
{{- end }}
ports:
{{- range $portNumber, $portInfo := $ports }}
  - name: {{ index $portInfo "name" }}
    containerPort: {{ $portNumber }}
    protocol: {{ index $portInfo "protocol" }}
{{- end }}
{{- end -}}

{{/*
Sets the configmap authentication options for a given realm.
*/}}
{{- define "polaris.authenticationOptions" -}}
{{- $realm := index . 0 -}}
{{- $map := index . 1 -}}
{{- $auth := index . 2 -}}
{{- $global := index . 3 -}}
{{- $prefix := empty $realm | ternary "polaris.authentication" (printf "polaris.authentication.\"%s\"" $realm) -}}
{{- $authType := coalesce $auth.type "internal" -}}
{{- if and (ne $authType "internal") (ne $authType "mixed") (ne $authType "external") -}}
{{- fail (empty $realm | ternary "authentication.type: invalid authentication type" (printf "authentication.realmOverrides.\"%s\".type: invalid authentication type" $realm)) -}}
{{- end -}}
{{- $_ := set $map (printf "%s.type" $prefix) $authType -}}
{{- $_ = set $map (printf "%s.authenticator.type" $prefix) (dig "authenticator" "type" "default" $auth) -}}
{{- if (or (eq $authType "mixed") (eq $authType "internal")) -}}
{{- $tokenBrokerType := dig "tokenBroker" "type" "rsa-key-pair" $auth -}}
{{- $_ = set $map (printf "%s.token-service.type" $prefix) (dig "tokenService" "type" "default" $auth) -}}
{{- $_ = set $map (printf "%s.token-broker.type" $prefix) $tokenBrokerType -}}
{{- $_ = set $map (printf "%s.token-broker.max-token-generation" $prefix) (dig "tokenBroker" "maxTokenGeneration" "PT1H" $auth) -}}
{{- $secretName := dig "tokenBroker" "secret" "name" "" $auth -}}
{{- if $secretName -}}
{{- $subpath := empty $realm | ternary "" (printf "%s/" (urlquery $realm)) -}}
{{- if eq $tokenBrokerType "rsa-key-pair" -}}
{{- $_ = set $map (printf "%s.token-broker.rsa-key-pair.public-key-file" $prefix) (printf "%s/%spublic.pem" $global.Values.image.configDir $subpath ) -}}
{{- $_ = set $map (printf "%s.token-broker.rsa-key-pair.private-key-file" $prefix) (printf "%s/%sprivate.pem" $global.Values.image.configDir $subpath ) -}}
{{- end -}}
{{- if eq $tokenBrokerType "symmetric-key" -}}
{{- $_ = set $map (printf "%s.token-broker.symmetric-key.file" $prefix) (printf "%s/%ssymmetric.key" $global.Values.image.configDir $subpath ) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Sets authentication options for a given realm in the projected config volume.
*/}}
{{- define "polaris.configVolumeAuthenticationOptions" -}}
{{- $realm := index . 0 -}}
{{- $auth := index . 1 -}}
{{- $global := index . 2 -}}
{{- $authType := coalesce $auth.type "internal" -}}
{{- if (or (eq $authType "mixed") (eq $authType "internal")) }}
{{- $secretName := dig "tokenBroker" "secret" "name" "" $auth -}}
{{- if $secretName -}}
{{- $tokenBrokerType := dig "tokenBroker" "type" "rsa-key-pair" $auth -}}
{{- $subpath := empty $realm | ternary "" (printf "%s/" (urlquery $realm)) -}}
- secret:
    name: {{ tpl $secretName $global }}
    items:
    {{- if eq $tokenBrokerType "rsa-key-pair" }}
      {{- /* Backward compatibility for publicKey: new takes precedence */ -}}
      {{- $publicKey := coalesce (dig "tokenBroker" "secret" "rsaKeyPair" "publicKey" "" $auth) (dig "tokenBroker" "secret" "publicKey" "public.pem" $auth) }}
      {{- /* Backward compatibility for privateKey: new takes precedence */ -}}
      {{- $privateKey := coalesce (dig "tokenBroker" "secret" "rsaKeyPair" "privateKey" "" $auth) (dig "tokenBroker" "secret" "privateKey" "private.pem" $auth) }}
      - key: {{ tpl $publicKey $global }}
        path: {{ $subpath }}public.pem
      - key: {{ tpl $privateKey $global }}
        path: {{ $subpath }}private.pem
    {{- end }}
    {{- if eq $tokenBrokerType "symmetric-key" }}
      {{- /* Backward compatibility for symmetricKey: new takes precedence */ -}}
      {{- $secretKey := coalesce (dig "tokenBroker" "secret" "symmetricKey" "secretKey" "" $auth) (dig "tokenBroker" "secret" "secretKey" "symmetric.key" $auth) }}
      - key: {{ tpl $secretKey $global }}
        path: {{ $subpath }}symmetric.key
    {{- end }}
{{- end }}
{{- end }}
{{- end -}}
