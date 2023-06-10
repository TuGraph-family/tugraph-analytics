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

package com.antgroup.geaflow.ha.service;

import java.io.Serializable;

public class ResourceData implements Serializable {

    private String host;
    private int processId;

    /** rpc service port.*/
    private int rpcPort;
    /** shuffle service port.*/
    private int shufflePort;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public int getRpcPort() {
        return rpcPort;
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

    @Override
    public String toString() {
        return "ResourceData{" + "host='" + host + '\'' + ", processId=" + processId + ", rpcPort="
            + rpcPort + ", shufflePort=" + shufflePort + '}';
    }
}
