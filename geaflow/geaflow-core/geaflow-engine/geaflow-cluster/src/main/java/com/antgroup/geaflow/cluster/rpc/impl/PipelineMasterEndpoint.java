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

import com.antgroup.geaflow.cluster.driver.Driver;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.rpc.IPipelineMasterEndpoint;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineMasterEndpoint implements IPipelineMasterEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineMasterEndpoint.class);

    private final Driver driver;

    public PipelineMasterEndpoint(Driver driver) {
        this.driver = driver;
    }

    @Override
    public Response process(Request request) {
        try {
            IEvent event = RpcMessageEncoder.decode(request.getPayload());
            driver.process(event);
            return Response.newBuilder().build();
        } catch (Throwable t) {
            LOGGER.error("process event failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("process event failed: %s", t.getMessage()), t);
        }
    }

}
