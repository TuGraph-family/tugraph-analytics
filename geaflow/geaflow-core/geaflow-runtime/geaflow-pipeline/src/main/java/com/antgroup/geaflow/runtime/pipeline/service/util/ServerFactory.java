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

package com.antgroup.geaflow.runtime.pipeline.service.util;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.pipeline.service.IPipelineServiceExecutorContext;
import com.antgroup.geaflow.pipeline.service.IServiceServer;
import com.antgroup.geaflow.pipeline.service.ServiceType;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerFactory {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerFactory.class);
    
    public static IServiceServer loadServer(IPipelineServiceExecutorContext context) {
        Configuration configuration = context.getConfiguration();
        String type = configuration.getString(FrameworkConfigKeys.SERVICE_SERVER_TYPE);
        ServiceLoader<IServiceServer> contextLoader = ServiceLoader.load(IServiceServer.class);
        Iterator<IServiceServer> contextIterable = contextLoader.iterator();
        while (contextIterable.hasNext()) {
            IServiceServer serviceServer = contextIterable.next();
            if (serviceServer.getServiceType() == ServiceType.getEnum(type)) {
                LOGGER.info("loaded IServiceServer implementation {}", serviceServer);
                serviceServer.init(context);
                return serviceServer;
            }
        }
        LOGGER.error("NOT found IServiceServer implementation with type:{}", type);
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IServiceServer.class.getSimpleName()));
    }
}
