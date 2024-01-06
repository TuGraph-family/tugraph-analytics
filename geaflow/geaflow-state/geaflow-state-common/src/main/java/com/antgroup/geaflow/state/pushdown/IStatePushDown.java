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

package com.antgroup.geaflow.state.pushdown;

import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.limit.IEdgeLimit;
import com.antgroup.geaflow.state.pushdown.project.IProjector;
import java.util.List;
import java.util.Map;

public interface IStatePushDown<K, T, R> {

    IFilter getFilter();

    <F extends IFilter> Map<K, F> getFilters();

    IEdgeLimit getEdgeLimit();

    List<EdgeAtom> getOrderFields();

    IProjector<T, R> getProjector();

    PushDownType getType();

    boolean isEmpty();
}
