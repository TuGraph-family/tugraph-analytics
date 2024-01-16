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

package com.antgroup.geaflow.analytics.service.client;

import com.antgroup.geaflow.common.rpc.HostAndPort;
import java.util.List;

public class AnalyticsServiceInfo {
    private String serverName;
    private final List<HostAndPort> coordinatorAddresses;
    private final int coordinatorNum;

    public AnalyticsServiceInfo(String serverName, List<HostAndPort> coordinatorAddresses) {
        this.serverName = serverName;
        this.coordinatorAddresses = coordinatorAddresses;
        this.coordinatorNum = coordinatorAddresses.size();
    }

    public AnalyticsServiceInfo(List<HostAndPort> coordinatorAddresses) {
        this.coordinatorAddresses = coordinatorAddresses;
        this.coordinatorNum = coordinatorAddresses.size();
    }

    public String getServerName() {
        return serverName;
    }

    public List<HostAndPort> getCoordinatorAddresses() {
        return coordinatorAddresses;
    }

    public HostAndPort getCoordinatorAddresses(int index) {
        return coordinatorAddresses.get(index);
    }

    public int getCoordinatorNum() {
        return coordinatorNum;
    }

}
