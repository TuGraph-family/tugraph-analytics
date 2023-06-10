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

import com.antgroup.geaflow.dsl.common.data.VirtualId;
import java.util.Objects;

public class BroadcastId implements VirtualId {

    private final int fromTaskIndex;

    public BroadcastId(int fromTaskIndex) {
        this.fromTaskIndex = fromTaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BroadcastId)) {
            return false;
        }
        BroadcastId that = (BroadcastId) o;
        return fromTaskIndex == that.fromTaskIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromTaskIndex);
    }
}
