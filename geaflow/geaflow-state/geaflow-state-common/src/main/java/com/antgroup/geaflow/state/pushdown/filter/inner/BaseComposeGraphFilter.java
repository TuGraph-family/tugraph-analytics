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

package com.antgroup.geaflow.state.pushdown.filter.inner;

import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseComposeGraphFilter extends BaseGraphFilter {

    protected List<IGraphFilter> childrenFilters;

    public BaseComposeGraphFilter(List<IGraphFilter> childrenFilters) {
        this.childrenFilters = childrenFilters;
    }

    @Override
    public DataType dateType() {
        return DataType.OTHER;
    }

    /**
     * If this returns true, the edge scan will terminate.
     */
    @Override
    public boolean dropAllRemaining() {
        for (IGraphFilter filter : childrenFilters) {
            if (!filter.dropAllRemaining()) {
                return false;
            }
        }
        return true;
    }

    public List<IGraphFilter> getFilterList() {
        return this.childrenFilters;
    }

    public List<IGraphFilter> cloneFilterList() {
        List<IGraphFilter> copyList = new ArrayList<>(childrenFilters.size());
        childrenFilters.forEach(c -> copyList.add(c.clone()));
        return copyList;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"").append(getFilterType().name()).append("\":[");
        if (childrenFilters != null && childrenFilters.size() > 0) {
            int size = childrenFilters.size();
            int index = 0;
            for (IFilter filter : childrenFilters) {
                sb.append(filter.toString());
                if (index < size - 1) {
                    sb.append(",");
                }
                index++;
            }
        }

        sb.append("]");
        sb.append("}");

        return sb.toString();
    }
}
