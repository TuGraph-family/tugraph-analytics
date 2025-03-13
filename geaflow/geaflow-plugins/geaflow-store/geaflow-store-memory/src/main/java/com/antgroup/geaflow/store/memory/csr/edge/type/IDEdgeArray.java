/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.store.memory.csr.edge.type;

import com.antgroup.geaflow.collection.array.PrimitiveArray;
import com.antgroup.geaflow.collection.array.PrimitiveArrayFactory;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDEdge;
import com.antgroup.geaflow.store.memory.csr.edge.IEdgeArray;
import java.util.ArrayList;
import java.util.List;

public class IDEdgeArray<K> implements IEdgeArray<K, Object> {

    private PrimitiveArray<K> dstIds;
    private PrimitiveArray<Byte> directions;

    public void init(Class<K> keyType, int capacity) {
        dstIds = PrimitiveArrayFactory.getCustomArray(keyType, capacity);
        directions = PrimitiveArrayFactory.getCustomArray(Byte.class, capacity);
    }

    @Override
    public List<IEdge<K, Object>> getRangeEdges(K sid, int start, int end) {
        List<IEdge<K, Object>> edges = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            edges.add(getEdge(sid, i));
        }
        return edges;
    }

    protected IEdge<K, Object> getEdge(K sid, int pos) {
        return new IDEdge<>(sid, getDstId(pos), getDirection(pos));
    }

    protected K getDstId(int pos) {
        return dstIds.get(pos);
    }

    protected EdgeDirection getDirection(int pos) {
        return EdgeDirection.values()[directions.get(pos)];
    }

    @Override
    public void drop() {
        dstIds = null;
        directions = null;
    }

    @Override
    public void set(int pos, IEdge<K, Object> edge) {
        dstIds.set(pos, edge.getTargetId());
        directions.set(pos, (byte)edge.getDirect().ordinal());
    }

}
