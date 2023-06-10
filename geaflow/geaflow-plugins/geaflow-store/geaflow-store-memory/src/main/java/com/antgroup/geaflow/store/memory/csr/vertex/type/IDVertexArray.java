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

package com.antgroup.geaflow.store.memory.csr.vertex.type;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.IDVertex;
import com.antgroup.geaflow.store.memory.csr.vertex.IVertexArray;
import java.util.BitSet;

public class IDVertexArray<K> implements IVertexArray<K, Object> {

    private int[] vId2EPos;
    protected BitSet nullVertexBitSet;

    @Override
    public void init(int capacity) {
        vId2EPos = new int[capacity + 1];
        nullVertexBitSet = new BitSet();
    }

    @Override
    public Tuple<Integer, Integer> getEdgePosRange(int pos) {
        if (pos < vId2EPos.length - 1) {
            return Tuple.of(vId2EPos[pos], vId2EPos[pos + 1]);
        }
        return Tuple.of(0, 0);
    }

    @Override
    public IVertex<K, Object> getVertex(K key, int pos) {
        return containsVertex(pos) ? new IDVertex<>(key) : null;
    }

    protected boolean containsVertex(int pos) {
        return !nullVertexBitSet.get(pos);
    }

    @Override
    public void drop() {
        vId2EPos = null;
    }

    @Override
    public void set(int pos, IVertex<K, Object> vertex) {
        if (vertex == null) {
            nullVertexBitSet.set(pos);
        }
        vId2EPos[pos + 1] = vId2EPos[pos];
    }

    @Override
    public int getNextPos(int pos) {
        return vId2EPos[pos + 1];
    }

    @Override
    public void updateVId2EPos(int pos) {
        vId2EPos[pos + 1] =  vId2EPos[pos + 1] + 1;
    }

}
