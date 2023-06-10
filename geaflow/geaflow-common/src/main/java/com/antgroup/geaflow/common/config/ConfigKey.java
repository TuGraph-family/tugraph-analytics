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

package com.antgroup.geaflow.common.config;

import java.io.Serializable;

public class ConfigKey implements Serializable {

    private final String key;
    private String description;
    private Object defaultValue;

    public ConfigKey(String key) {
        this.key = key;
    }

    public ConfigKey(String key, Object defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public ConfigKey description(String description) {
        this.description = description;
        return this;
    }

    public ConfigKey defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public String getKey() {
        return key;
    }

    public String getDescription() {
        return description;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }
}
