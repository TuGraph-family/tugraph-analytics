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

package com.antgroup.geaflow.state.pushdown.filter;

import com.antgroup.geaflow.state.data.OneDegreeGraph;

public class VertexMustContainFilter<K, VV, EV> implements IOneDegreeGraphFilter<K, VV, EV> {

    private static final VertexMustContainFilter filter = new VertexMustContainFilter();

    @Override
    public boolean filter(OneDegreeGraph<K, VV, EV> value) {
        return value.getVertex() != null;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.VERTEX_MUST_CONTAIN;
    }

    public static <K, VV, EV> VertexMustContainFilter<K, VV, EV> instance() {
        return filter;
    }

    @Override
    public String toString() {
        return getFilterType().name();
    }
}
