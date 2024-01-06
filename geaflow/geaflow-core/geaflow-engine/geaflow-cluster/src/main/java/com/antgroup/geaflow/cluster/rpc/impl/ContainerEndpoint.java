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

import com.antgroup.geaflow.cluster.container.Container;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerEvent;
import com.antgroup.geaflow.cluster.rpc.IContainerEndpoint;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerEndpoint implements IContainerEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerEndpoint.class);

    private final Container container;

    public ContainerEndpoint(Container workerContainer) {
        this.container = workerContainer;
    }

    @Override
    public Response process(Request request) {
        Response.Builder builder = Response.newBuilder();
        try {
            IEvent res;
            IEvent event = RpcMessageEncoder.decode(request.getPayload());
            if (event instanceof OpenContainerEvent) {
                res = container.open((OpenContainerEvent) event);
            } else {
                res = container.process(event);
            }
            if (res != null) {
                ByteString payload = RpcMessageEncoder.encode(res);
                builder.setPayload(payload);
            }
            return builder.build();
        } catch (Throwable t) {
            LOGGER.error("process request failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException("process request failed", t);
        }
    }

    @Override
    public Empty close(Empty request) {
        try {
            container.close();
            return Empty.newBuilder().build();
        } catch (Throwable t) {
            LOGGER.error("close failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("close failed: %s", t.getMessage()), t);
        }
    }
}
