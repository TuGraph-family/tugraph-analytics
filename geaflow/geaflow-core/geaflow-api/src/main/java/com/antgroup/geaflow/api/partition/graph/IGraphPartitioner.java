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

package com.antgroup.geaflow.api.partition.graph;

import com.antgroup.geaflow.api.partition.IPartition;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import java.io.Serializable;

public interface IGraphPartitioner<K, VV, EV,V extends IVertex<K,VV>,E extends IEdge<K,EV>>
    extends Serializable {

    IPartition<V> getVertexPartitioner();

    IPartition<E> getEdgePartitioner();

}
