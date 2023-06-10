/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.cluster.resourcemanager;

import java.io.Serializable;
import java.util.Objects;

public class WorkerInfo implements Comparable<WorkerInfo>, Serializable {

    private String host;
    private int rpcPort;
    private int shufflePort;
    private int processId;
    private int workerIndex;
    private String containerName;

    public WorkerInfo() {
    }

    public WorkerInfo(String host,
                      int rpcPort,
                      int shufflePort,
                      int processId,
                      int workerId,
                      String containerName) {
        this.host = host;
        this.rpcPort = rpcPort;
        this.shufflePort = shufflePort;
        this.processId = processId;
        this.workerIndex = workerId;
        this.containerName = containerName;
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getProcessId() {
        return this.processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public int getRpcPort() {
        return this.rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getShufflePort() {
        return shufflePort;
    }

    public void setShufflePort(int shufflePort) {
        this.shufflePort = shufflePort;
    }

    public int getWorkerIndex() {
        return this.workerIndex;
    }

    public void setWorkerIndex(int workerIndex) {
        this.workerIndex = workerIndex;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public WorkerId generateWorkerId() {
        return new WorkerId(this.containerName, this.workerIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkerInfo that = (WorkerInfo) o;
        return Objects.equals(this.containerName, that.containerName)
            && this.processId == that.processId
            && this.workerIndex == that.workerIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.containerName, this.processId, this.workerIndex);
    }

    @Override
    public int compareTo(WorkerInfo o) {
        int flag = this.containerName.compareTo(o.containerName);
        if (flag == 0) {
            flag = Integer.compare(this.processId, o.processId);
            if (flag == 0) {
                flag = Integer.compare(this.workerIndex, o.workerIndex);
            }
        }
        return flag;
    }

    @Override
    public String toString() {
        return "WorkerInfo{"
            + "host='" + host + '\''
            + ", rpcPort=" + rpcPort
            + ", shufflePort=" + shufflePort
            + ", processId=" + processId
            + ", workerIndex=" + workerIndex
            + ", containerName='" + containerName + '\''
            + '}';
    }

    public static WorkerInfo build(String host,
                                   int rpcPort,
                                   int shufflePort,
                                   int processId,
                                   int workerId,
                                   String containerName) {
        return new WorkerInfo(host, rpcPort, shufflePort, processId, workerId, containerName);
    }

    public static class WorkerId {

        private final String containerName;
        private final int workerIndex;

        public WorkerId(String containerName, int workerIndex) {
            this.containerName = containerName;
            this.workerIndex = workerIndex;
        }

        public String getContainerName() {
            return this.containerName;
        }

        public int getWorkerIndex() {
            return this.workerIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WorkerId workerId = (WorkerId) o;
            return Objects.equals(this.containerName, workerId.containerName) && this.workerIndex == workerId.workerIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.containerName, this.workerIndex);
        }

        @Override
        public String toString() {
            return this.containerName + '/' + this.workerIndex;
        }

    }

}
