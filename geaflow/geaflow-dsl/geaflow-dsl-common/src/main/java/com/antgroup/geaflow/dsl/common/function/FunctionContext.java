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

package com.antgroup.geaflow.dsl.common.function;

import com.antgroup.geaflow.common.config.Configuration;
import java.io.Serializable;
import java.util.Objects;

public class FunctionContext implements Serializable {

    private final Configuration config;

    private FunctionContext(Configuration config) {
        this.config = Objects.requireNonNull(config);
    }

    public static FunctionContext of(Configuration config) {
        return new FunctionContext(config);
    }

    public Configuration getConfig() {
        return config;
    }
}
