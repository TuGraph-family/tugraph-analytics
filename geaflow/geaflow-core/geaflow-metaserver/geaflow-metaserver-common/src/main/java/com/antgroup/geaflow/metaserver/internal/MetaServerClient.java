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

package com.antgroup.geaflow.metaserver.internal;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.metaserver.client.BaseClient;
import com.antgroup.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.response.DefaultResponse;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public class MetaServerClient extends BaseClient {

    private static MetaServerClient client;

    public MetaServerClient() {

    }

    public MetaServerClient(Configuration configuration) {
        super(configuration);
    }

    public static synchronized MetaServerClient getClient(Configuration configuration) {
        if (client == null) {
            client = new MetaServerClient(configuration);
        }
        return client;
    }

    public void registerService(NamespaceType namespaceType, String containerId,
                                HostAndPort serviceInfo) {
        DefaultResponse response = process(
            new RegisterServiceRequest(namespaceType, containerId, serviceInfo));
        if (!response.isSuccess()) {
            throw new GeaflowRuntimeException(response.getMessage());
        }
    }

    @Override
    public void close() {
        super.close();
        client = null;
    }
}
