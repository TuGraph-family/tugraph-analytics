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

package com.antgroup.geaflow.cluster.rpc.impl;

import com.antgroup.geaflow.cluster.rpc.ISupervisorEndpoint;
import com.antgroup.geaflow.cluster.runner.Supervisor;
import com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest;
import com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse;
import com.google.protobuf.Empty;

public class SupervisorEndpoint implements ISupervisorEndpoint {

    private final Supervisor supervisor;
    private final Empty empty;

    public SupervisorEndpoint(Supervisor supervisor) {
        this.supervisor = supervisor;
        this.empty = Empty.newBuilder().build();
    }

    @Override
    public Empty restart(RestartRequest request) {
        supervisor.restartWorker(request.getPid());
        return empty;
    }

    @Override
    public StatusResponse status(Empty empty) {
        boolean isAlive = supervisor.isWorkerAlive();
        return StatusResponse.newBuilder().setIsAlive(isAlive).build();
    }

}
