// @ts-ignore
/* eslint-disable */
import {request} from '@umijs/max';

/** 获取当前的集群Overview信息 GET /rest/overview */
export async function clusterOverview() {
  return request<{
    data?: API.ClusterOverview,
    success?: boolean,
    message?: string
  }>('/api/overview', {
    method: 'GET',
  });
}

/** 获取集群的作业列表 GET /api/jobs */
export async function jobList() {
  return request<{
    data?: API.GeaflowJob[],
    success?: boolean,
    message?: string
  }>('/api/jobs', {
    method: 'GET',
  });
}
