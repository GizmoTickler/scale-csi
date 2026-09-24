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
Get the complete driver image reference. An explicit digest is immutable and
takes precedence over the release tag.
*/}}
{{- define "scale-csi.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (include "scale-csi.imageTag" .) -}}
{{- end -}}
{{- end }}

{{/*
Merge a sidecar-specific security context over the hardened common baseline.
The explicit deep copy prevents mergeOverwrite from mutating .Values while the
same baseline is rendered for the other containers.
*/}}
{{- define "scale-csi.sidecarSecurityContext" -}}
{{- $context := deepCopy .common -}}
{{- if .override -}}
{{- $context = mergeOverwrite $context .override -}}
{{- end -}}
{{- toYaml $context -}}
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

{{/*
Convert a Go-style duration string composed of h/m/s segments (e.g. "1h",
"90m", "1h30m", "45s") to whole seconds. Used to derive the
ScaleCSIReconcileStalled age threshold from reconcile.interval so the alert
tracks the configured cadence instead of a hard-coded three hours (codex M3).
reconcile.interval is schema-restricted to ^([0-9]+(s|m|h))+$, so every segment
is parseable here and there is no ms/m ambiguity.
*/}}
{{- define "scale-csi.durationToSeconds" -}}
{{- $dur := toString . -}}
{{- $seconds := 0 -}}
{{- range regexFindAll "([0-9]+)h" $dur -1 -}}
{{- $seconds = add $seconds (mul (trimSuffix "h" . | int) 3600) -}}
{{- end -}}
{{- range regexFindAll "([0-9]+)m" $dur -1 -}}
{{- $seconds = add $seconds (mul (trimSuffix "m" . | int) 60) -}}
{{- end -}}
{{- range regexFindAll "([0-9]+)s" $dur -1 -}}
{{- $seconds = add $seconds (trimSuffix "s" . | int) -}}
{{- end -}}
{{- $seconds -}}
{{- end }}

{{/*
Convert a Go duration string to WHOLE SECONDS, rounding up, understanding every
unit the values schema permits (ns/us/µs/ms/s/m/h) plus fractional segments.

scale-csi.durationToSeconds above is deliberately left alone: it is pinned to
reconcile.interval, whose schema is ^([0-9]+(s|m|h))+$, and its segment-at-a-time
regexes mis-read anything outside that grammar ("500ms" matches its "([0-9]+)m"
arm and becomes 500 MINUTES). startupConnectTimeout's schema is the full Go
duration grammar, so it needs this parser instead.
*/}}
{{- define "scale-csi.durationToSecondsCeil" -}}
{{- $dur := toString . | trim -}}
{{- $seconds := 0.0 -}}
{{- range regexFindAll "[0-9]+(\\.[0-9]+)?(ns|us|µs|ms|h|m|s)" $dur -1 -}}
{{- $unit := regexFind "(ns|us|µs|ms|h|m|s)$" . -}}
{{- $value := float64 (trimSuffix $unit .) -}}
{{- if eq $unit "h" -}}
{{- $seconds = addf $seconds (mulf $value 3600.0) -}}
{{- else if eq $unit "m" -}}
{{- $seconds = addf $seconds (mulf $value 60.0) -}}
{{- else if eq $unit "s" -}}
{{- $seconds = addf $seconds $value -}}
{{- else if eq $unit "ms" -}}
{{- $seconds = addf $seconds (divf $value 1000.0) -}}
{{- else if eq $unit "ns" -}}
{{- $seconds = addf $seconds (divf $value 1000000000.0) -}}
{{- else -}}
{{- $seconds = addf $seconds (divf $value 1000000.0) -}}
{{- end -}}
{{- end -}}
{{- /* ceil returns a float64, and text/template prints a float64 with %v, i.e.
     strconv 'g' with shortest precision -- which switches to exponent form at
     an exponent of 6, so every value at or above 1e6 seconds rendered as
     "1.0008e+06" / "3.6e+06" instead of digits. Callers pipe the result
     through sprig's int/int64, both of which parse a string with ParseInt and
     return 0 on failure, so the controller's startup budget silently collapsed
     to its `max 30` floor for any startupConnectTimeout >= 1000000s ("278h",
     "300h", "1000h" are all legal per values.schema.json). Converting the
     float64 to int64 HERE, before it is ever stringified, is what fixes it:
     int64 of a float64 is a numeric conversion, not a string parse. */}}
{{- ceil $seconds | int64 -}}
{{- end }}

{{/*
The set of StorageClass entries this chart will actually RENDER, as a JSON
array. Takes the root context.

This is the single source of truth for "which classes exist", shared by
templates/storageclass.yaml (which renders them) and
templates/controller-rbac.yaml (which derives the Secret rule from them). They
used to each re-implement it, and they disagreed: the legacy singular
.Values.storageClass REPLACES the plural list here, but the RBAC copy ADDED to
it, so a plural class carrying chapSecretName/encryptionSecretName that the
legacy form had already displaced still switched on cluster-wide `get` on all
Secrets for a release that renders no class needing it.
*/}}
{{- define "scale-csi.storageClassList" -}}
{{- $classes := .Values.storageClasses -}}
{{- if .Values.storageClass -}}
  {{- /* The legacy defaults deliberately omit "protocol" and "mountOptions" so
       the deprecated path shares the modern omit-when-unset behavior below.
       Defaulting either to NFS values would corrupt an iscsi/nvmeof class. */}}
  {{- $legacy := mergeOverwrite (dict "create" true "name" "scale-nfs" "isDefault" false "reclaimPolicy" "Delete" "allowVolumeExpansion" true "volumeBindingMode" "Immediate" "extraParameters" (dict)) (deepCopy .Values.storageClass) -}}
  {{- if $legacy.create -}}
    {{- $classes = list $legacy -}}
  {{- else -}}
    {{- $classes = list -}}
  {{- end -}}
{{- end -}}
{{- /* An entry is rendered unless it explicitly sets enabled: false. This keeps
     every existing class rendering by default while letting opt-in example
     classes (e.g. the detached DR-restore class) ship disabled. */}}
{{- $enabledClasses := list -}}
{{- range $storageClass := $classes -}}
  {{- if or (not (hasKey $storageClass "enabled")) $storageClass.enabled -}}
    {{- $enabledClasses = append $enabledClasses $storageClass -}}
  {{- end -}}
{{- end -}}
{{- toJson $enabledClasses -}}
{{- end }}

{{/*
The rendered `parameters:` map for ONE StorageClass entry, as a JSON object.
Takes (dict "root" $root "class" $storageClass).

Shared with templates/controller-rbac.yaml for the same anti-drift reason as
scale-csi.storageClassList: the RBAC gate has to see the parameter map the
class actually ships, not a hand-maintained list of the values keys that were
known to produce a Secret reference when the gate was written. extraParameters
passes through verbatim, so a class can name a
csi.storage.k8s.io/*-secret-name directly without going through
chapSecretName/encryptionSecretName, and that used to render a StorageClass
with no matching RBAC rule at all.
*/}}
{{- define "scale-csi.storageClassParameters" -}}
{{- $root := .root -}}
{{- $storageClass := .class -}}
{{- $parameters := dict -}}
{{- with $storageClass.extraParameters -}}
  {{- $parameters = mergeOverwrite $parameters (deepCopy .) -}}
{{- end -}}
{{- /* Emit protocol only when the storageClass entry sets it explicitly.
     Omitting it lets the driver apply its sole-enabled-protocol fallback or
     return its missing-parameter error, instead of the chart silently forcing
     nfs on an iscsi/nvmeof class. */}}
{{- if $storageClass.protocol -}}
{{- $_ := set $parameters "protocol" $storageClass.protocol -}}
{{- end -}}
{{- /* Emit snapshotRestoreMode only when set, so unset classes follow the
     driver's global zfs.detachedVolumesFromSnapshots default. */}}
{{- if $storageClass.snapshotRestoreMode -}}
{{- $_ := set $parameters "snapshotRestoreMode" $storageClass.snapshotRestoreMode -}}
{{- end -}}
{{- /* Curated ZFS performance class. Emitted only when set; unset classes
     inherit the parent dataset's properties plus zfs.datasetProperties,
     exactly as before. volblocksize is CREATE-ONLY, so a class change that
     moves an existing zvol's geometry is rejected, not applied. */}}
{{- if $storageClass.zfsPerformanceClass -}}
{{- $_ := set $parameters "zfsPerformanceClass" $storageClass.zfsPerformanceClass -}}
{{- end -}}
{{- /* GF5 NFS export overrides. Each is emitted ONLY when the class sets it,
     so an untouched class renders the exact parameter map it did before and
     the driver keeps its historical create payload. */}}
{{- with $storageClass.nfsSecurity -}}
{{- $_ := set $parameters "nfsSecurity" (join "," .) -}}
{{- end -}}
{{- if hasKey $storageClass "nfsExposeSnapshots" -}}
{{- $_ := set $parameters "nfsExposeSnapshots" (printf "%v" $storageClass.nfsExposeSnapshots) -}}
{{- end -}}
{{- if hasKey $storageClass "nfsReadOnly" -}}
{{- $_ := set $parameters "nfsReadOnly" (printf "%v" $storageClass.nfsReadOnly) -}}
{{- end -}}
{{- /* Squash overrides, keyed on hasKey rather than truthiness: the driver
     distinguishes "parameter absent" (inherit the chart/global default) from
     "parameter present and EMPTY", and the empty string is the documented way
     to CLEAR the default root squash so a class can set mapall_* without
     tripping the maproot/mapall exclusivity. A truthiness test would silently
     drop exactly that value. */}}
{{- range $squashKey := (list "nfsMaprootUser" "nfsMaprootGroup" "nfsMapallUser" "nfsMapallGroup") -}}
{{- if hasKey $storageClass $squashKey -}}
{{- $_ := set $parameters $squashKey (printf "%v" (get $storageClass $squashKey)) -}}
{{- end -}}
{{- end -}}
{{- /* Export allowlists, comma-joined like nfsSecurity. Under
     fencing.mode=strict the driver REJECTS these at CreateVolume rather than
     discarding them, so a class that sets one must be on additive fencing. */}}
{{- with $storageClass.nfsAllowedNetworks -}}
{{- $_ := set $parameters "nfsAllowedNetworks" (join "," .) -}}
{{- end -}}
{{- with $storageClass.nfsAllowedHosts -}}
{{- $_ := set $parameters "nfsAllowedHosts" (join "," .) -}}
{{- end -}}
{{- /* NFSv4 ACL. Emitted only when set; see docs/reference/storageclass.md for
     the fsGroupPolicy=File interaction this opts a class into. */}}
{{- if $storageClass.nfsACLTemplate -}}
{{- $_ := set $parameters "nfsACLTemplate" $storageClass.nfsACLTemplate -}}
{{- end -}}
{{- if $storageClass.nfsACL -}}
{{- $_ := set $parameters "nfsACL" (toJson $storageClass.nfsACL) -}}
{{- end -}}
{{- /* aclmode. RESTRICTED is the documented primary mitigation for the
     ACL x fsGroup interaction and is the ONLY ZFS lever that stops a chmod
     from rewriting the ACL; the driver requires nfsACLTemplate or nfsACL
     alongside it. */}}
{{- if $storageClass.nfsACLMode -}}
{{- $_ := set $parameters "nfsACLMode" $storageClass.nfsACLMode -}}
{{- end -}}
{{- /* CHAP: when a per-StorageClass CHAP Secret is named, emit the four CSI
     secret-ref parameters (provisioner + node-stage). The chart references
     the Secret by name/namespace only; credential values never appear here.
     The namespace defaults to the release namespace when left unset. */}}
{{- if $storageClass.chapSecretName -}}
{{- $chapSecretNamespace := $storageClass.chapSecretNamespace | default $root.Release.Namespace -}}
{{- $_ := set $parameters "csi.storage.k8s.io/provisioner-secret-name" $storageClass.chapSecretName -}}
{{- $_ := set $parameters "csi.storage.k8s.io/provisioner-secret-namespace" $chapSecretNamespace -}}
{{- $_ := set $parameters "csi.storage.k8s.io/node-stage-secret-name" $storageClass.chapSecretName -}}
{{- $_ := set $parameters "csi.storage.k8s.io/node-stage-secret-namespace" $chapSecretNamespace -}}
{{- end -}}
{{- /* Encryption (GF-Sprint 1): when a per-StorageClass encryption Secret is
     named, emit the encryption parameter AND the three CSI secret-ref
     parameters — provisioner-secret (CreateVolume), controller-publish-secret
     (the publish-time unlock and the locked-volume reconciler's Secret
     resolution) and node-stage-secret (parity with the CHAP block). The chart
     references the Secret by name/namespace only; the passphrase never appears
     here. The namespace defaults to the release namespace when left unset.
     Encryption is create-time only: the driver refuses an encrypted create that
     also carries a content source, so there is deliberately no chart-level
     snapshotRestoreMode coupling. */}}
{{- if $storageClass.encryptionSecretName -}}
{{- $encryptionSecretNamespace := $storageClass.encryptionSecretNamespace | default $root.Release.Namespace -}}
{{- $_ := set $parameters "encryption" "true" -}}
{{- $_ := set $parameters "csi.storage.k8s.io/provisioner-secret-name" $storageClass.encryptionSecretName -}}
{{- $_ := set $parameters "csi.storage.k8s.io/provisioner-secret-namespace" $encryptionSecretNamespace -}}
{{- $_ := set $parameters "csi.storage.k8s.io/controller-publish-secret-name" $storageClass.encryptionSecretName -}}
{{- $_ := set $parameters "csi.storage.k8s.io/controller-publish-secret-namespace" $encryptionSecretNamespace -}}
{{- $_ := set $parameters "csi.storage.k8s.io/node-stage-secret-name" $storageClass.encryptionSecretName -}}
{{- $_ := set $parameters "csi.storage.k8s.io/node-stage-secret-namespace" $encryptionSecretNamespace -}}
{{- end -}}
{{- toJson $parameters -}}
{{- end }}

{{/*
Whether the controller's sidecars need `get` on Secrets for a given rendered
StorageClass parameter map, as "true"/"" — i.e. whether the class names a
CONTROLLER-side CSI secret ref.

Only provisioner-secret (external-provisioner: CreateVolume/DeleteVolume),
controller-publish-secret (external-attacher) and controller-expand-secret
(external-resizer) are read with the controller ServiceAccount's credentials.
node-stage-secret / node-publish-secret / node-expand-secret are resolved by
the kubelet and arrive in the RPC already populated, so they need no rule
here and must not widen this grant.
*/}}
{{- define "scale-csi.storageClassNeedsControllerSecretGet" -}}
{{- $needed := "" -}}
{{- range $key, $value := . -}}
{{- if regexMatch "^csi\\.storage\\.k8s\\.io/(provisioner|controller-publish|controller-expand)-secret-name$" $key -}}
{{- $needed = "true" -}}
{{- end -}}
{{- end -}}
{{- $needed -}}
{{- end }}

{{/*
scale-csi.tombstoneBacklogThreshold renders metrics.prometheusRule.tombstoneBacklogThreshold.
`default` cannot be used: Sprig treats a numeric 0 as empty, so an explicit 0
("alert on any sustained backlog") silently became 500. Only an absent key takes
the default.
*/}}
{{- define "scale-csi.tombstoneBacklogThreshold" -}}
{{- $rule := .Values.metrics.prometheusRule -}}
{{- if and (hasKey $rule "tombstoneBacklogThreshold") (not (kindIs "invalid" $rule.tombstoneBacklogThreshold)) -}}
{{- $rule.tombstoneBacklogThreshold | int -}}
{{- else -}}
500
{{- end -}}
{{- end -}}

{{/*
Non-empty when NVMe-oF volumes may use the userspace (ublk) data path: NVMe-oF
is enabled and either nvmeof.ublk.enabled or nvmeof.dataPath=ublk. Gates the
driver's ublk config and the node plugin's /run/nvmeublk mount. Null-safe for a
deleted nvmeof or nvmeof.ublk subtree.
*/}}
{{- define "scale-csi.nvmeofUblkInUse" -}}
{{- $nvmeof := .Values.nvmeof | default dict -}}
{{- $ublk := $nvmeof.ublk | default dict -}}
{{- if and $nvmeof.enabled (or $ublk.enabled (eq ($nvmeof.dataPath | default "kernel") "ublk")) -}}
true
{{- end -}}
{{- end }}

{{/*
The nvmeublkd image reference. A tag or a digest is required: no nvmeublk image
is published with this chart, so there is no default to fall back to.
*/}}
{{- define "scale-csi.nvmeublkdImage" -}}
{{- $image := .image | default dict -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $image.repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $image.repository (required "nvmeof.ublk.daemon.image.tag (or digest) is required when nvmeof.ublk.daemon.enabled=true: no nvmeublk image is published with this chart" $image.tag) -}}
{{- end -}}
{{- end }}
