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

package com.antgroup.geaflow.dsl.runtime.traversal.path;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractSingleTreePath extends AbstractTreePath {

    /**
     * The request ids of this node belongs to.
     */
    protected Set<Object> requestIds;

    @Override
    public void setRequestId(Object requestId) {
        if (requestId == null) {
            requestIds = null;
        } else {
            requestIds = Sets.newHashSet(requestId);
        }
    }

    @Override
    public Set<Object> getRequestIds() {
        return requestIds;
    }

    @Override
    public void addRequestIds(Collection<Object> requestIds) {
        if (requestIds == null) {
            return;
        }
        if (this.requestIds == null) {
            this.requestIds = new HashSet<>();
        }
        this.requestIds.addAll(requestIds);
    }
}
