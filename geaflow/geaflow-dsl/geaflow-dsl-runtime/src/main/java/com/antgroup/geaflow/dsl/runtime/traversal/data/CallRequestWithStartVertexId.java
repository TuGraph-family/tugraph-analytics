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

import java.io.Serializable;
import java.util.Objects;

public class CallRequestWithStartVertexId implements Serializable {

    private final Object requestId;

    private final long callOpId;

    private final Object startVertexId;

    public CallRequestWithStartVertexId(Object requestId, long callOpId, Object startVertexId) {
        this.requestId = requestId;
        this.callOpId = callOpId;
        this.startVertexId = startVertexId;
    }

    public Object getRequestId() {
        return requestId;
    }

    public long getCallOpId() {
        return callOpId;
    }

    public Object getStartVertexId() {
        return startVertexId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CallRequestWithStartVertexId)) {
            return false;
        }
        CallRequestWithStartVertexId that = (CallRequestWithStartVertexId) o;
        return callOpId == that.callOpId && Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, callOpId);
    }

    @Override
    public String toString() {
        return "CallRequestWithStartVertexId{"
            + "requestId=" + requestId
            + ", callOpId=" + callOpId
            + ", startVertexId=" + startVertexId
            + '}';
    }
}
