{{- define "daos-csi.fullname" -}}
{{- default "daos-csi" .Values.fullnameOverride | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "daos-csi.labels" -}}
app.kubernetes.io/name: daos-csi
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "daos-csi.image" -}}
{{- if not .Values.image.repository -}}
{{- fail "image.repository is required (the daos-csi image built from the Containerfile)" -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "daos-csi.agentImage" -}}
{{- if .Values.agent.image.repository -}}
{{- printf "%s:%s" .Values.agent.image.repository (default .Chart.AppVersion .Values.agent.image.tag) -}}
{{- else -}}
{{- include "daos-csi.image" . -}}
{{- end -}}
{{- end -}}

{{- define "daos-csi.metricsAnnotations" -}}
{{- if and .root.Values.metrics.enabled .root.Values.metrics.podAnnotations }}
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: {{ .port | quote }}
  prometheus.io/path: /metrics
{{- end }}
{{- end -}}

{{- define "daos-csi.httpArgs" -}}
{{- if .root.Values.metrics.enabled }}
- -http-endpoint=:{{ .port }}
{{- end }}
- -log-level={{ .root.Values.log.level }}
- -log-format={{ .root.Values.log.format }}
{{- end -}}

{{- define "daos-csi.probes" -}}
{{- if .root.Values.metrics.enabled }}
ports:
- {name: metrics, containerPort: {{ .port }}}
livenessProbe:
  httpGet: {path: /healthz, port: {{ .port }}}
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: {path: /readyz, port: {{ .port }}}
  periodSeconds: 5
{{- end }}
{{- end -}}
