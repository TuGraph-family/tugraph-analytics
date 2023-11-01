// @ts-ignore
/* eslint-disable */
import {request} from '@umijs/max';

/** 获取当前的集群Overview信息 GET /rest/overview */
export async function clusterOverview() {
  return request<{
    data?: API.ClusterOverview,
    success?: boolean,
    message?: string
  }>('/rest/overview', {
    method: 'GET',
  });
}

/** 获取集群所有Driver的信息 GET /rest/drivers */
export async function driverInfos() {
  return request<{
    data?: API.ComponentInfo[],
    success?: boolean,
    message?: string
  }>('/rest/drivers', {
    method: 'GET',
  });
}

/** 获取集群所有Container的信息 GET /rest/containers */
export async function containerInfos() {
  return request<{
    data?: API.ComponentInfo[],
    success?: boolean,
    message?: string
  }>('/rest/containers', {
    method: 'GET',
  });
}

/** 获取集群Master的配置表 GET /rest/master/configuration */
export async function masterConfiguration() {
  return request<{
    data?: Record<string, any>,
    success?: boolean,
    message?: string
  }>('/rest/master/configuration', {
    method: 'GET',
  });
}

/** 获取集群Master的进程Metrics GET /rest/master/metrics */
export async function masterMetrics() {
  return request<{
    data?: API.ProcessMetrics,
    success?: boolean,
    message?: string
  }>('/rest/master/metrics', {
    method: 'GET',
  });
}

/** 获取集群Master的基础信息 GET /rest/master/info */
export async function masterInfo() {
  return request<{
    data?: API.ComponentInfo,
    success?: boolean,
    message?: string
  }>('/rest/master/info', {
    method: 'GET',
  });
}

/** 获取集群所有的Pipeline Metrics GET /rest/pipelines */
export async function pipelineList() {
  return request<{
    data?: API.PipelineMetrics[],
    success?: boolean,
    message?: string
  }>('/rest/pipelines', {
    method: 'GET'
  });
}

/** 获取集群单个Pipeline所有的Cycle Metrics GET /rest/pipelines/{pipelineName}/cycles */
export async function cycleList(pipelineName?: string) {
  return request<{
    data?: API.CycleMetrics[],
    success?: boolean,
    message?: string
  }>('/rest/pipelines/' + pipelineName + '/cycles', {
    method: 'GET',
  });
}

export async function logList(agentUrl: string) {
  return request<{
    data: API.FileInfo[],
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/logs", {
    method: 'GET',
  });
}

export async function getLogContent(agentUrl: string, logPath: string, pageNo: number, pageSize: number) {
  return request<{
    data?: API.PageResponse<string>,
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/logs/content", {
    method: 'GET',
    params: {
      path: logPath,
      pageNo: pageNo,
      pageSize: pageSize
    }
  });
}

export async function flameGraphList(agentUrl: string) {
  return request<{
    data: API.FileInfo[],
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/flame-graphs", {
    method: 'GET',
  });
}

export async function getFlameGraphContent(agentUrl: string, filePath: string) {
  return request<{
    data?: string,
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/flame-graphs/content", {
    method: 'GET',
    params: {
      path: filePath
    }
  });
}

export async function executeFlameGraph(agentUrl: string, body: API.FlameGraphRequest) {
  return request<{
    data?: string,
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/flame-graphs", {
    method: 'POST',
    data: body
  });
}

export async function deleteFlameGraph(agentUrl: string, path: string) {
  return request<{
    data?: string,
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/flame-graphs", {
    method: 'DELETE',
    params: {
      path: path
    }
  });
}

export async function getThreadDumpContent(agentUrl: string, pageNo: number, pageSize: number) {
  return request<{
    data?: API.PageResponse<API.ThreadDumpResponse>,
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/thread-dump/content", {
    method: 'GET',
    params: {
      pageNo: pageNo,
      pageSize: pageSize
    }
  });
}

export async function executeThreadDump(agentUrl: string, pid: number) {
  return request<{
    data?: string,
    success?: boolean,
    message?: string
  }>('/proxy/' + agentUrl + "/rest/thread-dump", {
    method: 'POST',
    data: {
      pid
    } as API.ThreadDumpRequest
  });
}
