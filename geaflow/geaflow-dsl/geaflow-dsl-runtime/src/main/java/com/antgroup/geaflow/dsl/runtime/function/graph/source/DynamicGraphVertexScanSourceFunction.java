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

package com.antgroup.geaflow.dsl.runtime.function.graph.source;

import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.graph.StateMode;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.Iterator;

public class DynamicGraphVertexScanSourceFunction<K> extends AbstractVertexScanSourceFunction<K> {

    public DynamicGraphVertexScanSourceFunction(GraphViewDesc graphViewDesc) {
        super(graphViewDesc);
    }

    @Override
    protected Iterator<K> buildIdIterator() {
        return graphState.dynamicGraph().V().idIterator();
    }

    @Override
    protected GraphStateDescriptor<K, ?, ?> buildGraphStateDesc() {
        GraphStateDescriptor<K, ?, ?> desc =  super.buildGraphStateDesc();
        desc.withDataModel(DataModel.DYNAMIC_GRAPH);
        desc.withStateMode(StateMode.RW);
        desc.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        return desc;
    }
}
