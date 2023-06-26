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

package com.antgroup.geaflow.dsl.common.util;

import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;

public class Windows {

    public static final long SIZE_OF_ALL_WINDOW = -1L;

    public static <T> IWindow<T> createWindow(Configuration configuration) {
        long batchSize = configuration.getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE);
        if (batchSize == SIZE_OF_ALL_WINDOW) {
            return AllWindow.getInstance();
        }
        return new SizeTumblingWindow<>(batchSize);
    }
}
