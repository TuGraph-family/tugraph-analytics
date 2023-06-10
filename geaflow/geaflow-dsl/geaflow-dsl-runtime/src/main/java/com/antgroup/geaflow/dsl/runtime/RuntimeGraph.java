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

package com.antgroup.geaflow.dsl.runtime;

import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.rel.GraphAlgorithm;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import java.util.List;

/**
 * The runtime graph view which mapping logical graph operator to the runtime
 * representation of the underlying engine.
 */
public interface RuntimeGraph extends RDataView {

    List<Path> take();

    RuntimeGraph traversal(GraphMatch graphMatch);

    RuntimeTable getPathTable();

    RuntimeTable runAlgorithm(GraphAlgorithm graphAlgorithm);

    default ViewType getType() {
        return ViewType.GRAPH;
    }
}
