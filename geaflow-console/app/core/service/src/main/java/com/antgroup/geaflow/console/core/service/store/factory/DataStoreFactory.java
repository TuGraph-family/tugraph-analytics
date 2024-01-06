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

package com.antgroup.geaflow.console.core.service.store.factory;

import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.service.PluginService;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.store.impl.PersistentDataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class DataStoreFactory {

    @Autowired
    ApplicationContext context;

    @Autowired
    PluginService pluginService;

    public GeaflowDataStore getDataStore(String type) {
        GeaflowPluginType typeEnum = GeaflowPluginType.of(type);
        switch (typeEnum) {
            case LOCAL:
            case DFS:
            case OSS:
                return context.getBean(PersistentDataStore.class);
            default:
                throw new GeaflowIllegalException("Not supported data store type {}", type);
        }
    }

}
