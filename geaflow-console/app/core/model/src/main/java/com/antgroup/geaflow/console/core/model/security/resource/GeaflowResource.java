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

package com.antgroup.geaflow.console.core.model.security.resource;

import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.google.common.base.Preconditions;
import lombok.Getter;

@Getter
public abstract class GeaflowResource {

    protected final GeaflowResourceType type;

    protected final GeaflowResource parent;

    protected GeaflowResource(GeaflowResourceType type, GeaflowResource parent) {
        this.type = Preconditions.checkNotNull(type, "Invalid resource type");
        this.parent = parent;
    }

    public abstract String getId();

    public abstract boolean include(GeaflowResource other);
}
