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
Get the image tag
*/}}
{{- define "scale-csi.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion }}
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
/var/lib/kubelet
{{- end }}
