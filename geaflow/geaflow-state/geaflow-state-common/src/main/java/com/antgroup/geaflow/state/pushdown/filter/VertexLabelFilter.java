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

import com.antgroup.geaflow.model.graph.IGraphElementWithLabelField;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Set;

public class VertexLabelFilter<K, VV> implements IVertexFilter<K, VV> {

    private Set<String> labels;

    public VertexLabelFilter(Collection<String> list) {
        this.labels = Sets.newHashSet(list);
    }

    public VertexLabelFilter(String... labels) {
        this.labels = Sets.newHashSet(labels);
    }

    public static <K, VV> VertexLabelFilter<K, VV> instance(String... labels) {
        return new VertexLabelFilter<>(labels);
    }

    @Override
    public boolean filter(IVertex<K, VV> value) {
        return labels.contains(((IGraphElementWithLabelField)value).getLabel());
    }

    public Set<String> getLabels() {
        return labels;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.VERTEX_LABEL;
    }

    @Override
    public String toString() {
        return String.format("\"%s(%s)\"", getFilterType().toString(),
            Joiner.on(',').join(labels));
    }
}
