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

package com.antgroup.geaflow.cluster.rpc;

import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import java.io.Serializable;
import java.util.List;

public interface IMasterEndpointRef extends Serializable {

    /**
     * Register container into master.
     */
    <T> void registerContainer(T request, RpcCallback<RegisterResponse> listener);

    /**
     * Send heartbeat.
     */
    ListenableFuture sendHeartBeat(Heartbeat request);

    /**
     * Send exception.
     */
    Empty sendException(Integer containerId, String message);

    /**
     * Get container info.
     */
    List<ContainerInfo> getContainerInfo(List<String> containerIds);

}
