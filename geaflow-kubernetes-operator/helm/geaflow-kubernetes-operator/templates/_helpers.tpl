################################################################################
#  Copyright 2023 AntGroup CO., Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
################################################################################

{{/*
Expand the name of the chart.
*/}}
{{- define "geaflow-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "geaflow-operator.fullname" -}}
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
{{- define "geaflow-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "geaflow-operator.labels" -}}
helm.sh/chart: {{ include "geaflow-operator.chart" . }}
{{ include "geaflow-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "geaflow-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "geaflow-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the operator role to use
*/}}
{{- define "geaflow-operator.roleName" -}}
{{- if .Values.rbac.operatorRole.create }}
{{- default (include "geaflow-operator.fullname" .) .Values.rbac.operatorRole.name }}
{{- else }}
{{- default "default" .Values.rbac.operatorRole.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the operator role binding to use
*/}}
{{- define "geaflow-operator.roleBindingName" -}}
{{- if .Values.rbac.operatorRoleBinding.create }}
{{- default (include "geaflow-operator.fullname" .) .Values.rbac.operatorRoleBinding.name }}
{{- else }}
{{- default "default" .Values.rbac.operatorRoleBinding.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the job role to use
*/}}
{{- define "geaflow-operator.jobRoleName" -}}
{{- if .Values.rbac.jobRoleBinding.create }}
{{- default (include "geaflow-operator.fullname" .) .Values.rbac.jobRole.name }}
{{- else }}
{{- default "default" .Values.rbac.jobRole.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the job role to use
*/}}
{{- define "geaflow-operator.jobRoleBindingName" -}}
{{- if .Values.rbac.jobRole.create }}
{{- default (include "geaflow-operator.fullname" .) .Values.rbac.jobRoleBinding.name }}
{{- else }}
{{- default "default" .Values.rbac.jobRoleBinding.name }}
{{- end }}
{{- end }}


{{/*
Create the name of the operator service account to use
*/}}
{{- define "geaflow-operator.serviceAccountName" -}}
{{- if .Values.operatorServiceAccount.create }}
{{- default (include "geaflow-operator.fullname" .) .Values.operatorServiceAccount.name }}
{{- else }}
{{- default "default" .Values.operatorServiceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the job service account to use
*/}}
{{- define "geaflow-operator.jobServiceAccountName" -}}
{{- if .Values.jobServiceAccount.create }}
{{- default (include "geaflow-operator.fullname" .) .Values.jobServiceAccount.name }}
{{- else }}
{{- default "default" .Values.jobServiceAccount.name }}
{{- end }}
{{- end }}

{{/*
Determine role scope based on name
*/}}
{{- define "flink-operator.roleScope" -}}
{{- if contains ":" .role  }}
{{- printf "ClusterRole" }}
{{- else }}
{{- printf "Role" }}
{{- end }}
{{- end }}