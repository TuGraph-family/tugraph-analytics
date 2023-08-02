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

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.rpc.IPipelineManagerEndpointRef;
import com.antgroup.geaflow.rpc.proto.Container;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class PipelineMasterEndpointRef extends ContainerEndpointRef implements
    IPipelineManagerEndpointRef {

    public PipelineMasterEndpointRef(String host, int port, ExecutorService executorService) {
        super(host, port, executorService);
    }

    @Override
    public Future<IEvent> process(IEvent request) {
        ensureChannelAlive();
        Container.Request taskEvent = buildRequest(request);
        blockingStub.process(taskEvent);
        return null;
    }
}
