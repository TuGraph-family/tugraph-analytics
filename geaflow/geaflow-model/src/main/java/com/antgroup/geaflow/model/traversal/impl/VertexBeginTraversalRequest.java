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

package com.antgroup.geaflow.model.traversal.impl;

import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.TraversalType.RequestType;

public class VertexBeginTraversalRequest<K> implements ITraversalRequest<K> {

    private K vId;

    public VertexBeginTraversalRequest(K vId) {
        this.vId = vId;
    }

    @Override
    public long getRequestId() {
        return this.vId.hashCode();
    }

    @Override
    public K getVId() {
        return vId;
    }


    @Override
    public RequestType getType() {
        return RequestType.Vertex;
    }
}
