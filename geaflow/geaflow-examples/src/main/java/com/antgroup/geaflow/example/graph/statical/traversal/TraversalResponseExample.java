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

package com.antgroup.geaflow.example.graph.statical.traversal;

import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.model.traversal.TraversalType;

public class TraversalResponseExample<T> implements ITraversalResponse<T> {

    private long responseId;
    private T response;

    public TraversalResponseExample(long responseId, T response) {
        this.responseId = responseId;
        this.response = response;
    }

    @Override
    public long getResponseId() {
        return responseId;
    }

    @Override
    public T getResponse() {
        return response;
    }

    @Override
    public TraversalType.ResponseType getType() {
        return TraversalType.ResponseType.Vertex;
    }

    @Override
    public String toString() {
        return "TraversalResponse{" + "responseId=" + responseId + ", response=" + response
            + '}';
    }
}
