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

package com.antgroup.geaflow.cluster.exception;

import com.antgroup.geaflow.cluster.rpc.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionClient.class);

    private static ExceptionClient INSTANCE;

    private final int containerId;
    private final String masterId;

    public ExceptionClient(int containerId, String masterId) {
        this.containerId = containerId;
        this.masterId = masterId;
    }

    public static synchronized ExceptionClient init(int containerId, String masterId) {
        if (INSTANCE == null) {
            INSTANCE = new ExceptionClient(containerId, masterId);
        }
        return INSTANCE;
    }

    public static synchronized ExceptionClient getInstance() {
        return INSTANCE;
    }

    public void sendException(Throwable throwable) {
        try {
            LOGGER.info("Send exception {} to master.", throwable.getMessage());
            RpcClient.getInstance().sendException(masterId, containerId, throwable);
        } catch (Throwable e) {
            LOGGER.error("Send exception {} to master failed.", throwable.getMessage(), e);
        }
    }

}
