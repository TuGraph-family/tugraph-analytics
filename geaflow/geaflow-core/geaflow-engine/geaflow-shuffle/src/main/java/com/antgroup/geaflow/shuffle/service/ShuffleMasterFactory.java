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

package com.antgroup.geaflow.shuffle.service;

import com.antgroup.geaflow.common.config.Configuration;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleMasterFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleMasterFactory.class);

    public static IShuffleMaster getShuffleMaster(Configuration configuration) {
        for (IShuffleMaster shuffleMaster : ServiceLoader.load(IShuffleMaster.class)) {
            shuffleMaster.init(configuration);
            return shuffleMaster;
        }
        LOGGER.warn("no shuffle master impl found");
        return null;
    }

}
