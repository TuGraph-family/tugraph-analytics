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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.dsl.common.data.Row;
import java.io.Serializable;
import java.util.Objects;

public class ParameterRequest implements Serializable {

    private final Object requestId;

    private final Object vertexId;

    private final Row parameters;

    public ParameterRequest(Object requestId, Object vertexId, Row parameters) {
        this.requestId = requestId;
        this.vertexId = vertexId;
        this.parameters = parameters;
    }

    public Object getRequestId() {
        return requestId;
    }

    public Object getVertexId() {
        return vertexId;
    }

    public Row getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ParameterRequest)) {
            return false;
        }
        ParameterRequest that = (ParameterRequest) o;
        return Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId);
    }

    @Override
    public String toString() {
        return "ParameterRequest{"
            + "requestId=" + requestId
            + ", vertexId=" + vertexId
            + ", parameters=" + parameters
            + '}';
    }
}
