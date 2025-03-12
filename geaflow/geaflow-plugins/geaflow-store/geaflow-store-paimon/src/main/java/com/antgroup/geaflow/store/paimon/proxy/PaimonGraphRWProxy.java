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

package com.antgroup.geaflow.store.paimon.proxy;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.store.paimon.PaimonTableRWHandle;
import com.antgroup.geaflow.store.paimon.iterator.PaimonIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.utils.Filter;

public class PaimonGraphRWProxy<K, VV, EV> extends PaimonBaseGraphProxy<K, VV, EV> {

    public PaimonGraphRWProxy(PaimonTableRWHandle vertexHandle, PaimonTableRWHandle edgeHandle,
                              int[] projection,
                              IGraphKVEncoder<K, VV, EV> encoder) {
        super(vertexHandle, edgeHandle, projection, encoder);
    }

    @Override
    public void archive(long checkpointId) {
        this.lastCheckpointId = checkpointId;
        this.vertexHandle.commit(lastCheckpointId);
        this.edgeHandle.commit(lastCheckpointId);
    }

    @Override
    public void recover(long checkpointId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long recoverLatest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        Filter<InternalRow> filter = row -> Arrays.equals(row.getBinary(KEY_COLUMN_INDEX), key);
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(filter,
            projection);
        try (PaimonIterator paimonIterator = new PaimonIterator(iterator)) {
            if (paimonIterator.hasNext()) {
                Tuple<byte[], byte[]> row = paimonIterator.next();
                IVertex<K, VV> vertex = encoder.getVertexEncoder()
                    .getVertex(row.getF0(), row.getF1());
                if (pushdown == null || ((IGraphFilter) pushdown.getFilter()).filterVertex(
                    vertex)) {
                    return vertex;
                }
            }
            return null;
        }
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        byte[] prefixBytes = encoder.getEdgeEncoder().getScanBytes(sid);
        BinaryString prefix = BinaryString.fromBytes(prefixBytes);
        Filter<InternalRow> filter = row -> {
            BinaryString key = row.getString(KEY_COLUMN_INDEX);
            return key.startsWith(prefix);
        };
        RecordReaderIterator<InternalRow> iterator = this.edgeHandle.getIterator(filter,
            projection);
        List<IEdge<K, EV>> edges = new ArrayList<>();
        try (PaimonIterator paimonIterator = new PaimonIterator(iterator)) {
            IGraphFilter graphFilter = GraphFilter.of(pushdown.getFilter(),
                pushdown.getEdgeLimit());
            while (paimonIterator.hasNext()) {
                Tuple<byte[], byte[]> row = paimonIterator.next();
                IEdge<K, EV> edge = encoder.getEdgeEncoder().getEdge(row.getF0(), row.getF1());
                if (graphFilter.filterEdge(edge)) {
                    edges.add(edge);
                }
                if (graphFilter.dropAllRemaining()) {
                    break;
                }
            }
            return edges;
        }
    }
}
