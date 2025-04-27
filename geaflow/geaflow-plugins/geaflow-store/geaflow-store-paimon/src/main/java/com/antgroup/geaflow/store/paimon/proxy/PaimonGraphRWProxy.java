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

package com.antgroup.geaflow.store.paimon.proxy;

import static java.util.Collections.singletonList;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.store.paimon.PaimonTableRWHandle;
import com.antgroup.geaflow.store.paimon.iterator.PaimonIterator;
import com.antgroup.geaflow.store.paimon.predicate.BytesStartsWith;
import java.util.ArrayList;
import java.util.List;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.RowType;

public class PaimonGraphRWProxy<K, VV, EV> extends PaimonBaseGraphProxy<K, VV, EV> {

    public PaimonGraphRWProxy(PaimonTableRWHandle vertexHandle, PaimonTableRWHandle edgeHandle,
                              int[] projection, IGraphKVEncoder<K, VV, EV> encoder) {
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
        RowType rowType = vertexHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(Equal.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(key));
        RecordReaderIterator<InternalRow> iterator = this.vertexHandle.getIterator(predicate, null,
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
        RowType rowType = edgeHandle.getTable().rowType();
        Predicate predicate = new LeafPredicate(BytesStartsWith.INSTANCE, rowType.getTypeAt(0), 0,
            rowType.getField(0).name(), singletonList(prefixBytes));
        RecordReaderIterator<InternalRow> iterator = this.edgeHandle.getIterator(predicate, null,
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
