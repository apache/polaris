{{/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
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
{{- if ne $value nil -}}
{{- $valAsString = tpl (toString $value) $global -}}
{{- if contains "\r\n" $valAsString -}}
{{- $valAsString = $valAsString | nindent 4 | replace "\r\n" "\\\r\n" -}}
{{- else if contains "\n" $valAsString -}}
{{- $valAsString = $valAsString | nindent 4 | replace "\n" "\\\n" -}}
{{- end -}}
{{- end -}}
{{ print $key "=" $valAsString }}
{{- end -}}

{{/*
Convert a dict into a string formed by a comma-separated list of key-value pairs: key1=value1,key2=value2, ...
*/}}
{{- define "polaris.dictToString" -}}
{{- $list := list -}}
{{- range $k, $v := . -}}
{{- $list = append $list (printf "%s=%s" $k $v) -}}
{{- end -}}
{{ join "," $list }}
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
      {{- if .Values.authentication.tokenBroker.secret.name }}
      - secret:
          name: {{ tpl .Values.authentication.tokenBroker.secret.name . }}
          items:
          {{- if eq .Values.authentication.tokenBroker.type "rsa-key-pair" }}
            - key: {{ tpl .Values.authentication.tokenBroker.secret.publicKey . }}
              path: public.pem
            - key: {{ tpl .Values.authentication.tokenBroker.secret.privateKey . }}
              path: private.pem
          {{- end }}
          {{- if eq .Values.authentication.tokenBroker.type "symmetric-key" }}
            - key: {{ tpl .Values.authentication.tokenBroker.secret.secretKey . }}
              path: symmetric.key
          {{- end }}
      {{- end }}
      {{- if and ( eq .Values.persistence.type "eclipse-link" ) .Values.persistence.eclipseLink.secret.name }}
      - secret:
          name: {{ tpl .Values.persistence.eclipseLink.secret.name . }}
          items:
            - key: {{ tpl .Values.persistence.eclipseLink.secret.key . }}
              path: persistence.xml
      {{- end }}
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
Quantities should be positive, so negative values, zero, or any unparseable number
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
Prints the ports section of the container spec. Also validates all port names to ensure
that they are unique.
*/}}
{{- define "polaris.containerPorts" -}}
{{- $ports := dict -}}
{{- $protocols := dict -}}
{{- /* Main service ports */ -}}
{{- range $i, $port := .Values.service.ports -}}
{{- if hasKey $ports $port.name -}}
{{- fail (printf "service.ports[%d]: port name already taken: %v" $i $port.name) -}}
{{- end -}}
{{- $portNumber := coalesce $port.targetPort $port.port -}}
{{- $_ := set $ports $port.name $portNumber -}}
{{- $_ = set $protocols $port.name ($port.protocol | default "TCP") -}}
{{- end -}}
{{- /* Management service ports */ -}}
{{- range $i, $port := .Values.managementService.ports -}}
{{- if hasKey $ports $port.name -}}
{{- fail (printf "managementService.ports[%d]: port name already taken: %v" $i $port.name) -}}
{{- end -}}
{{- $portNumber := coalesce $port.targetPort $port.port -}}
{{- $_ := set $ports $port.name $portNumber }}
{{- $_ = set $protocols $port.name ($port.protocol | default "TCP") -}}
{{- end -}}
{{- /* Extra service ports */ -}}
{{- range $i, $svc := .Values.extraServices -}}
{{- range $j, $port := $svc.ports -}}
{{- $portNumber := coalesce $port.targetPort $port.port -}}
{{- if hasKey $ports $port.name -}}
{{- if ne $portNumber (get $ports $port.name) -}}
{{- fail (printf "extraServices[%d].ports[%d]: wrong port number for port %s, expected %v, got %v" $i $j $port.name (get $ports $port.name) $portNumber) -}}
{{- end -}}
{{- end -}}
{{- if hasKey $protocols $port.name -}}
{{- if ne ($port.protocol | default "TCP") (get $protocols $port.name) -}}
{{- fail (printf "extraServices[%d].ports[%d]: wrong protocol for port %s, expected %v, got %v" $i $j $port.name (get $protocols $port.name) $port.protocol) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $ports $port.name $portNumber -}}
{{- $_ = set $protocols $port.name ($port.protocol | default "TCP") -}}
{{- end -}}
{{- end }}
ports:
{{- range $portName, $portNumber := $ports }}
  - name: {{ $portName }}
    containerPort: {{ $portNumber }}
    protocol: {{ get $protocols $portName }}
{{- end }}
{{- end -}}
