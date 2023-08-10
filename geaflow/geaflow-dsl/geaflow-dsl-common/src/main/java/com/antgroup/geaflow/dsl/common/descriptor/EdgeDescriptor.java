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

package com.antgroup.geaflow.dsl.common.descriptor;

import java.util.Objects;

public class EdgeDescriptor {

    public String id;
    public String type;
    public String sourceType;
    public String targetType;

    public EdgeDescriptor(String id, String type, String sourceType, String targetType) {
        this.id = Objects.requireNonNull(id);
        this.type = Objects.requireNonNull(type);
        this.sourceType = Objects.requireNonNull(sourceType);
        this.targetType = Objects.requireNonNull(targetType);
    }
}
