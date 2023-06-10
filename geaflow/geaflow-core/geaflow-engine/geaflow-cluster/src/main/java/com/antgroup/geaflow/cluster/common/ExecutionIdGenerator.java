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

package com.antgroup.geaflow.cluster.common;

import com.antgroup.geaflow.common.utils.IdGenerator;

public class ExecutionIdGenerator {

    private static volatile ExecutionIdGenerator INSTANCE;

    private IdGenerator idGenerator;

    private ExecutionIdGenerator(int containerId) {
        this.idGenerator = new IdGenerator(containerId);
    }

    public static ExecutionIdGenerator init(int containerId) {
        if (INSTANCE == null) {
            synchronized (ExecutionIdGenerator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ExecutionIdGenerator(containerId);
                }
            }
        }
        return INSTANCE;
    }

    public static ExecutionIdGenerator getInstance() {
        return INSTANCE;
    }

    public long generateId() {
        return idGenerator.nextId();
    }

}
