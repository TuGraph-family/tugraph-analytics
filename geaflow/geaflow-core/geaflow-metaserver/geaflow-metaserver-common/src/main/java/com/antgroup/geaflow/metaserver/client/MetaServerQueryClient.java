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

package com.antgroup.geaflow.metaserver.client;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.request.QueryAllServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.response.ServiceResponse;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import java.util.List;

public class MetaServerQueryClient extends BaseClient {

    private static MetaServerQueryClient client;

    public MetaServerQueryClient(Configuration configuration) {
        super(configuration);
    }

    public static synchronized MetaServerQueryClient getClient(Configuration configuration) {
        if (client == null) {
            client = new MetaServerQueryClient(configuration);
        }
        return client;
    }


    public List<HostAndPort> queryAllServices(NamespaceType namespaceType) {
        ServiceResponse response = process(new QueryAllServiceRequest(namespaceType));
        if (!response.isSuccess()) {
            throw new GeaflowRuntimeException(response.getMessage());
        }
        return response.getServiceInfos();
    }

    @Override
    public void close() {
        super.close();
        client = null;
    }
}
