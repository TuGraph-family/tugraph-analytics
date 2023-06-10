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

package com.antgroup.geaflow.view.graph;

import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.view.PView;
import java.util.Iterator;
import java.util.ServiceLoader;

public interface PGraphView<K, VV, EV> extends PView {

    /**
     * Load and initialize graph view.
     */
    static <K, VV, EV> PGraphView<K, VV, EV> loadPGraphView(GraphViewDesc graphViewDesc) {
        ServiceLoader<PGraphView> contextLoader = ServiceLoader.load(PGraphView.class);
        Iterator<PGraphView> graphViewIterable = contextLoader.iterator();
        while (graphViewIterable.hasNext()) {
            PGraphView<K, VV, EV> pGraphView = graphViewIterable.next();
            pGraphView.init(graphViewDesc);
            return pGraphView;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(PGraphView.class.getSimpleName()));
    }

    /**
     * Initialize graph view by desc.
     */
    PGraphView<K, VV, EV> init(GraphViewDesc graphViewDesc);

    /**
     * Append vertex stream into incremental graphView.
     */
    PIncGraphView<K, VV, EV> appendVertex(PWindowStream<IVertex<K, VV>> vertexStream);

    /**
     * Append edge stream into incremental graphView.
     */
    PIncGraphView<K, VV, EV> appendEdge(PWindowStream<IEdge<K, EV>> edgeStream);

    /**
     * Append vertex/edge stream into incremental graphView.
     */
    PIncGraphView<K, VV, EV> appendGraph(PWindowStream<IVertex<K, VV>> vertexStream,
                                        PWindowStream<IEdge<K, EV>> edgeStream);

    /**
     * Build graph snapshot of specified version.
     */
    PGraphWindow<K, VV, EV> snapshot(long version);

}
