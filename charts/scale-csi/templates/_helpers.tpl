{{/*
Expand the name of the chart.
*/}}
{{- define "scale-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "scale-csi.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "scale-csi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "scale-csi.labels" -}}
helm.sh/chart: {{ include "scale-csi.chart" . }}
{{ include "scale-csi.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "scale-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "scale-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Controller selector labels
*/}}
{{- define "scale-csi.controllerSelectorLabels" -}}
{{ include "scale-csi.selectorLabels" . }}
app.kubernetes.io/component: controller
{{- end }}

{{/*
Node selector labels
*/}}
{{- define "scale-csi.nodeSelectorLabels" -}}
{{ include "scale-csi.selectorLabels" . }}
app.kubernetes.io/component: node
{{- end }}

{{/*
Create the name of the service account for the controller
*/}}
{{- define "scale-csi.controllerServiceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (printf "%s-controller" (include "scale-csi.fullname" .)) .Values.serviceAccount.controllerName }}
{{- else }}
{{- default "default" .Values.serviceAccount.controllerName }}
{{- end }}
{{- end }}

{{/*
Create the name of the service account for the node
*/}}
{{- define "scale-csi.nodeServiceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (printf "%s-node" (include "scale-csi.fullname" .)) .Values.serviceAccount.nodeName }}
{{- else }}
{{- default "default" .Values.serviceAccount.nodeName }}
{{- end }}
{{- end }}

{{/*
Create the name of the secret
*/}}
{{- define "scale-csi.secretName" -}}
{{- if .Values.truenas.existingSecret }}
{{- .Values.truenas.existingSecret }}
{{- else }}
{{- include "scale-csi.fullname" . }}
{{- end }}
{{- end }}

{{/*
Get the image tag.
The published image is v-prefixed (ci.yml tags v{{version}}), but the release
workflow strips the "v" from Chart.appVersion. So default to "v<appVersion>" to
match the real image; an explicitly-set .Values.image.tag is used verbatim.
*/}}
{{- define "scale-csi.imageTag" -}}
{{- if .Values.image.tag }}{{ .Values.image.tag }}{{ else }}{{ printf "v%s" .Chart.AppVersion }}{{ end }}
{{- end }}

{{/*
Get the CSI driver name
*/}}
{{- define "scale-csi.driverName" -}}
{{- .Values.csiDriverName }}
{{- end }}

{{/*
Get the CSI socket path
*/}}
{{- define "scale-csi.socketPath" -}}
/csi/csi.sock
{{- end }}

{{/*
Get the CSI socket directory
*/}}
{{- define "scale-csi.socketDir" -}}
/csi
{{- end }}

{{/*
Get the kubelet directory
*/}}
{{- define "scale-csi.kubeletDir" -}}
{{- .Values.kubeletDir }}
{{- end }}
