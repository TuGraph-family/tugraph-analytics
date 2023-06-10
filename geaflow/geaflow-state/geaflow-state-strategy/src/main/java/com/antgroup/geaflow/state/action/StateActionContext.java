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

package com.antgroup.geaflow.state.action;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.store.IBaseStore;

public class StateActionContext {

    private IBaseStore baseStore;
    private Configuration config;

    public StateActionContext(IBaseStore baseStore, Configuration config) {
        this.baseStore = baseStore;
        this.config = config;
    }

    public IBaseStore getBaseStore() {
        return baseStore;
    }

    public Configuration getConfig() {
        return config;
    }
}
