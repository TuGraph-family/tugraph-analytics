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

package com.antgroup.geaflow.dsl.common.compile;

import java.util.HashMap;
import java.util.Map;

public class CompileContext {

    private Map<String, String> config;

    private Map<String, Integer> parallelisms;

    public CompileContext() {
        this(new HashMap<>(), new HashMap<>());
    }

    public CompileContext(Map<String, String> config, Map<String, Integer> parallelisms) {
        this.config = config;
        this.parallelisms = parallelisms;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public Map<String, Integer> getParallelisms() {
        return parallelisms;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public void setParallelisms(Map<String, Integer> parallelisms) {
        this.parallelisms = parallelisms;
    }
}
