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

package com.antgroup.geaflow.cluster.client;

import com.antgroup.geaflow.cluster.client.utils.PipelineUtil;
import com.antgroup.geaflow.cluster.rpc.ConnectAddress;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineClientFactory.class);

    public static IPipelineClient createPipelineClient(Map<String, ConnectAddress> driverAddresses, Configuration config) {
        ServiceLoader<IPipelineClient> clientLoader = ServiceLoader.load(IPipelineClient.class);
        Iterator<IPipelineClient> clientIterable = clientLoader.iterator();
        boolean isSync = !PipelineUtil.isAsync(config);
        while (clientIterable.hasNext()) {
            IPipelineClient client = clientIterable.next();
            if (client.isSync() == isSync) {
                client.init(driverAddresses, config);
                return client;
            }
        }
        LOGGER.error("NOT found IPipelineClient implementation");
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IPipelineClient.class.getSimpleName()));
    }
}
