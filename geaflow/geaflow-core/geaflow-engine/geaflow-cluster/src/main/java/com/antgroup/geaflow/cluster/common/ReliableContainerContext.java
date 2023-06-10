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

import com.antgroup.geaflow.common.config.Configuration;
import java.io.Serializable;

public abstract class ReliableContainerContext implements IReliableContext, Serializable {

    protected int id;
    protected Configuration config;
    protected boolean isRecover;

    public ReliableContainerContext(int id, Configuration config) {
        this.id = id;
        this.config = config;
    }

    public int getId() {
        return id;
    }

    public Configuration getConfig() {
        return config;
    }

    public boolean isRecover() {
        return isRecover;
    }

    @Override
    public synchronized void checkpoint(IReliableContext.IReliableContextCheckpointFunction function) {
        function.doCheckpoint(this);
    }
}
