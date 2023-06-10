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

package com.antgroup.geaflow.cluster.container;

import com.antgroup.geaflow.cluster.common.ComponentInfo;

public class ContainerInfo extends ComponentInfo {

    /** shuffle service port.*/
    private int shufflePort;

    public ContainerInfo() {
    }

    public ContainerInfo(int containerId, String containerName, String host, int pid, int rpcPort,
                         int shufflePort) {
        super(containerId, containerName, host, pid, rpcPort);
        this.shufflePort = shufflePort;
    }

    public int getShufflePort() {
        return shufflePort;
    }

    public void setShufflePort(int shufflePort) {
        this.shufflePort = shufflePort;
    }

    @Override
    public String toString() {
        return "ContainerInfo{" + "id=" + id + ", name='" + name + '\'' + ", host='" + host + '\''
            + ", pid=" + pid + ", rpcPort=" + rpcPort + ", shufflePort=" + shufflePort + "}";
    }
}
