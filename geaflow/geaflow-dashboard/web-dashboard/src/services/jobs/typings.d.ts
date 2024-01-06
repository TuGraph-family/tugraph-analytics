// @ts-ignore
/* eslint-disable */

declare namespace API {

  type CurrentUser = {
    name?: string,
    avatar?: string
  }

  type ClusterOverview = {
    totalContainers?: number;
    activeContainers?: number;
    totalDrivers?:number;
    activeDrivers?:number;
    totalWorkers?: number;
    availableWorkers?: number;
    pendingWorkers?: number;
    usedWorkers?: number;
  }

  type MasterConfig = {
    name?: string;
    value?: any;
  }

  type ProcessMetrics = {
    heapCommittedMB?: number;
    heapUsedMB?: number;
    heapUsedRatio?: number;
    totalMemoryMB?: number;
    fgcCount?: number;
    fgcTime?: number;
    gcTime?: number;
    gcCount?: number;
    avgLoad?: number;
    availCores?: number;
    processCpu?: number;
    usedCores?: number;
    activeThreads?: number;
  }

  type ComponentInfo = {
    id?: number;
    name?: string;
    host?: string;
    pid?: number;
    agentPort?: number;
    lastTimestamp?: number;
    isActive?: boolean;
    metrics?: ProcessMetrics;
  } & ProcessMetrics;

  type PipelineMetrics = {
    name?: string;
    duration?: number;
    startTime?: number;
  };

  type CycleMetrics = {
    name?: string;
    pipelineName?: string;
    opName?: string;
    duration?: number;
    startTime?: number;
    totalTasks?: number;
    slowestTask?: number;
    slowestTaskExecuteTime?: number;
    inputRecords?: number;
    inputKb?: number;
    outputRecords?: number;
    outputKb?: number;
    avgGcTime?: number;
    avgExecuteTime?: number;
  };

  type FileInfo = {
    path?: string;
    createdTime?: number;
    size?: number;
  }

  type PageRequest = {
    pageNo: number,
    pageSize: number
  }

  type PageResponse<T> = {
    total: number;
    data: T;
  }

  type FlameGraphRequest = {
    type?: string;
    duration?: number;
    pid?: number;
  }

  type ThreadDumpRequest = {
    pid: number
  }

  type ThreadDumpResponse = {
    lastDumpTime?: number;
    content?: string;
  }

}
