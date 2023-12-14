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

package com.antgroup.geaflow.ha.leaderelection;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElectionServiceFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionServiceFactory.class);

    public static ILeaderElectionService loadElectionService(Configuration configuration) {
        String leaderElectionServiceType = configuration.getString(ExecutionConfigKeys.LEADER_ELECTION_TYPE);
        ServiceLoader<ILeaderElectionService> contextLoader = ServiceLoader.load(ILeaderElectionService.class);
        Iterator<ILeaderElectionService> contextIterable = contextLoader.iterator();
        while (contextIterable.hasNext()) {
            ILeaderElectionService service = contextIterable.next();
            if (service.getType().name().equalsIgnoreCase(leaderElectionServiceType)) {
                LOGGER.info("load ILeaderElectionService implementation {} with type {}",
                    service.getClass().getName(), leaderElectionServiceType);
                return service;
            }
        }
        LOGGER.error("Not found ILeaderElectionService implementation with type {}", leaderElectionServiceType);
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(ILeaderElectionService.class.getSimpleName()));
    }

}
