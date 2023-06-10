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

package com.antgroup.geaflow.api.function.io;

import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.model.graph.GraphRecord;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

public interface GraphSourceFunction<K, VV, EV> extends SourceFunction<GraphRecord<IVertex<K, VV>, IEdge<K, EV>>> {

    /**
     * Fetch vertex/edge data from source by window, and collect data by ctx.
     */
    @Override
    default boolean fetch(IWindow<GraphRecord<IVertex<K, VV>, IEdge<K, EV>>> window,
                          SourceContext<GraphRecord<IVertex<K, VV>, IEdge<K, EV>>> ctx) throws Exception {
        return fetch(window.windowId(), (GraphSourceContext<K, VV, EV>) ctx);
    }

    boolean fetch(long windowId, GraphSourceContext<K, VV, EV> ctx) throws Exception;

    interface GraphSourceContext<K, VV, EV> extends SourceContext<GraphRecord<IVertex<K, VV>,
        IEdge<K, EV>>> {

        /**
         * Partition vertex.
         */
        void collectVertex(IVertex<K, VV> vertex) throws Exception;

        /**
         * Partition edge.
         */
        void collectEdge(IEdge<K, EV> edge) throws Exception;
    }
}
