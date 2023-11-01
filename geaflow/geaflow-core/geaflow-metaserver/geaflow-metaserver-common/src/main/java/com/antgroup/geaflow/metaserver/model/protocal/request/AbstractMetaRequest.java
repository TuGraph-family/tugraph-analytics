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

package com.antgroup.geaflow.metaserver.model.protocal.request;

import com.antgroup.geaflow.metaserver.model.protocal.MetaRequest;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public abstract class AbstractMetaRequest implements MetaRequest {

    private final NamespaceType nameSpaceType;

    public AbstractMetaRequest(NamespaceType nameSpaceType) {
        this.nameSpaceType = nameSpaceType;
    }

    @Override
    public NamespaceType namespaceType() {
        return nameSpaceType;
    }
}
