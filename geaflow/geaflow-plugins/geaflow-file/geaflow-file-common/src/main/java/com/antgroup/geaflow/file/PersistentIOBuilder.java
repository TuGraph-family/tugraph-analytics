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

package com.antgroup.geaflow.file;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.ServiceLoader;

public class PersistentIOBuilder {

    public static IPersistentIO build(Configuration userConfig) {
        PersistentType type = PersistentType.valueOf(
            userConfig.getString(FileConfigKeys.PERSISTENT_TYPE).toUpperCase());
        ServiceLoader<IPersistentIO> serviceLoader = ServiceLoader.load(IPersistentIO.class);
        for (IPersistentIO persistentIO: serviceLoader) {
            if (persistentIO.getPersistentType() == type) {
                persistentIO.init(userConfig);
                return persistentIO;
            }
        }

        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(type.toString()));
    }

}
