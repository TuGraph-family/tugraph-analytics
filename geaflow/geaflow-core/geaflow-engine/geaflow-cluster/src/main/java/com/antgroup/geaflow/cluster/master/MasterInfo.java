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

package com.antgroup.geaflow.cluster.master;

import com.antgroup.geaflow.cluster.common.ComponentInfo;

public class MasterInfo extends ComponentInfo {
    private int httpPort;

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    @Override
    public String toString() {
        return "MasterInfo{" + "httpPort=" + httpPort + ", id=" + id + ", name='" + name + '\''
            + ", host='" + host + '\'' + ", pid=" + pid + ", rpcPort=" + rpcPort + ", metricPort="
            + metricPort + ", agentPort=" + agentPort + "} " + super.toString();
    }
}
