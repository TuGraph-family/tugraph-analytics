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

package com.antgroup.geaflow.operator.impl.graph.algo.vc.context.statical;

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;

public class StaticTraversalVertexQueryImpl<K, VV, EV> extends StaticVertexQueryImpl<K, VV, EV>
    implements TraversalVertexQuery<K, VV> {

    public StaticTraversalVertexQueryImpl(K vertexId, GraphState<K, VV, EV> graphState, KeyGroup keyGroup) {
        super(vertexId, graphState, keyGroup);
    }

    @Override
    public CloseableIterator<K> loadIdIterator() {
        if (keyGroup == null) {
            return graphState.staticGraph().V().idIterator();
        }
        return graphState.staticGraph().V().query(keyGroup).idIterator();
    }
}
