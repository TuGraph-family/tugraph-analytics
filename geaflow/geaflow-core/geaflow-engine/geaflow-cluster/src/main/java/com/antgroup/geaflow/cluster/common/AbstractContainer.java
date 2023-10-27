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

package com.antgroup.geaflow.cluster.common;

import com.antgroup.geaflow.cluster.exception.ExceptionClient;
import com.antgroup.geaflow.cluster.exception.ExceptionCollectService;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatClient;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;

public abstract class AbstractContainer extends AbstractComponent {

    protected HeartbeatClient heartbeatClient;
    protected ExceptionCollectService exceptionCollectService;

    public AbstractContainer(int rpcPort) {
        super(rpcPort);
    }

    @Override
    public void init(int id, String name, Configuration configuration) {
        super.init(id, name, configuration);

        startRpcService();
        ShuffleManager.init(configuration);
        ExceptionClient.init(id, name, masterId);
        this.heartbeatClient = new HeartbeatClient(id, name, configuration);
        this.exceptionCollectService = new ExceptionCollectService();
    }

    protected void registerToMaster() {
        this.heartbeatClient.registerToMaster(masterId, buildComponentInfo());
    }

    protected abstract void startRpcService();

    protected abstract ComponentInfo buildComponentInfo();

    protected void buildComponentInfo(ComponentInfo componentInfo) {
        componentInfo.setId(id);
        componentInfo.setName(name);
        componentInfo.setHost(ProcessUtil.getHostIp());
        componentInfo.setPid(ProcessUtil.getProcessId());
        componentInfo.setRpcPort(rpcPort);
    }

    @Override
    public void close() {
        super.close();
        if (exceptionCollectService != null) {
            exceptionCollectService.shutdown();
        }
        if (heartbeatClient != null) {
            heartbeatClient.close();
        }
    }

}
