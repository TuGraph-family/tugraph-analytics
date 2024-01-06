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

import com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatResponse;
import com.antgroup.geaflow.rpc.proto.Master.RegisterRequest;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.baidu.brpc.client.RpcCallback;
import java.util.concurrent.Future;

public interface IAsyncMasterEndpoint extends IMasterEndpoint {

    /**
     * Async register container.
     */
    Future<RegisterResponse> registerContainer(RegisterRequest request, RpcCallback<RegisterResponse> callback);

    /**
     * Async register driver.
     */
    Future<RegisterResponse> registerDriver(RegisterRequest request, RpcCallback<RegisterResponse> callback);

    /**
     * Async receive heartbeat.
     */
    Future<HeartbeatResponse> receiveHeartbeat(HeartbeatRequest request, RpcCallback<HeartbeatResponse> callback);

}

