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

package com.antgroup.geaflow.infer.exchange.serialize;

public class DictionaryConstructor implements IObjectConstructor {
    private final String module;
    private final String name;

    public DictionaryConstructor(String module, String name) {
        this.module = module;
        this.name = name;
    }

    public Object construct(Object[] args) {
        if (args.length > 0) {
            throw new PickleException("expected zero arguments for construction of ClassDict (for " + module + "." + name
                + "). This happens when an unsupported/unregistered class is being unpickled that"
                + " requires construction arguments. Fix it by registering a custom IObjectConstructor for this class.");
        }
        return new Dictionary(module, name);
    }
}

