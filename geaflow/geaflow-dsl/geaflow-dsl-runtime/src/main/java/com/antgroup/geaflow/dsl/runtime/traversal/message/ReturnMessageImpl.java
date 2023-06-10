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

package com.antgroup.geaflow.dsl.runtime.traversal.message;

import com.antgroup.geaflow.dsl.runtime.traversal.data.SingleValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ReturnMessageImpl implements ReturnMessage {

    private final Map<ReturnKey, SingleValue> returnKey2Value;

    public ReturnMessageImpl(Map<ReturnKey, SingleValue> returnKey2Value) {
        this.returnKey2Value = returnKey2Value;
    }

    public ReturnMessageImpl() {
        this(new HashMap<>());
    }

    @Override
    public MessageType getType() {
        return MessageType.RETURN_VALUE;
    }

    @Override
    public ReturnMessage combine(IMessage other) {
        ReturnMessage copy = this.copy();
        ReturnMessage otherMsg = (ReturnMessage) other;

        for (Map.Entry<ReturnKey, SingleValue> entry : otherMsg.getReturnKey2Values().entrySet()) {
            ReturnKey returnKey = entry.getKey();
            SingleValue value = entry.getValue();
            if (value != null) {
                copy.putValue(returnKey, value);
            }
        }
        return copy;
    }

    @Override
    public ReturnMessage copy() {
        return new ReturnMessageImpl(new HashMap<>(returnKey2Value));
    }

    @Override
    public Map<ReturnKey, SingleValue> getReturnKey2Values() {
        return returnKey2Value;
    }

    @Override
    public void putValue(ReturnKey returnKey, SingleValue value) {
        returnKey2Value.put(returnKey, value);
    }

    @Override
    public SingleValue getValue(ReturnKey returnKey) {
        return returnKey2Value.get(returnKey);
    }


    public static class ReturnKey implements Serializable {

        private final Object requestId;

        private final long queryId;

        public ReturnKey(Object requestId, long queryId) {
            this.requestId = requestId;
            this.queryId = queryId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ReturnKey)) {
                return false;
            }
            ReturnKey returnKey = (ReturnKey) o;
            return queryId == returnKey.queryId && Objects.equals(requestId, returnKey.requestId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId, queryId);
        }

        @Override
        public String toString() {
            return "ReturnKey{"
                + "requestId=" + requestId
                + ", queryId=" + queryId
                + '}';
        }
    }
}
