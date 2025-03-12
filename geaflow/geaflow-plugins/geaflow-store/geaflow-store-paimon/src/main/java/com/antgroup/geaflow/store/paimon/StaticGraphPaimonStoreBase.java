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

package com.antgroup.geaflow.store.paimon;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.graph.encoder.GraphKVEncoderFactory;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.store.api.graph.IStaticGraphStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.paimon.proxy.IGraphPaimonProxy;
import com.antgroup.geaflow.store.paimon.proxy.PaimonProxyBuilder;
import java.util.List;
import java.util.Map;
import org.apache.paimon.catalog.Identifier;

public class StaticGraphPaimonStoreBase<K, VV, EV> extends BasePaimonGraphStore implements
    IStaticGraphStore<K, VV, EV> {

    private EdgeAtom sortAtom;

    private IGraphPaimonProxy<K, VV, EV> proxy;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        int[] projection = new int[]{KEY_COLUMN_INDEX, VALUE_COLUMN_INDEX};

        // TODO: Use graph schema to create table instead of KV table.
        String vertexTableName = String.format("%s#%s", "vertex", shardId);
        Identifier vertexIdentifier = new Identifier(paimonStoreName, vertexTableName);
        PaimonTableRWHandle vertexHandle = createKVTableHandle(vertexIdentifier);

        String edgeTableName = String.format("%s#%s", "edge", shardId);
        Identifier edgeIdentifier = new Identifier(paimonStoreName, edgeTableName);
        PaimonTableRWHandle edgeHandle = createKVTableHandle(edgeIdentifier);

        this.sortAtom = storeContext.getGraphSchema().getEdgeAtoms().get(1);
        IGraphKVEncoder<K, VV, EV> encoder = GraphKVEncoderFactory.build(storeContext.getConfig(),
            storeContext.getGraphSchema());
        this.proxy = PaimonProxyBuilder.build(storeContext.getConfig(), vertexHandle, edgeHandle,
            projection, encoder);
    }

    @Override
    public void archive(long checkpointId) {
        this.proxy.archive(checkpointId);
    }

    @Override
    public void recovery(long checkpointId) {
        // TODO: Not implemented yet.
        this.proxy.recover(checkpointId);
    }

    @Override
    public long recoveryLatest() {
        return this.proxy.recoverLatest();
    }

    @Override
    public void compact() {

    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        this.proxy.addEdge(edge);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        this.proxy.addVertex(vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        return this.proxy.getVertex(sid, pushdown);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return this.proxy.getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        checkOrderField(pushdown.getOrderFields());
        return this.proxy.getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return this.proxy.vertexIDIterator();
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        return this.proxy.vertexIDIterator(pushDown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return this.proxy.getVertexIterator(pushdown);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys,
                                                               IStatePushDown pushdown) {
        return this.proxy.getVertexIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        return this.proxy.getEdgeIterator(pushdown);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return this.proxy.getEdgeIterator(keys, pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        return this.proxy.getOneDegreeGraphIterator(pushdown);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return this.proxy.getOneDegreeGraphIterator(keys, pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return this.proxy.getEdgeProjectIterator(pushdown);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K,
                                                                         EV>, R> pushdown) {
        return this.proxy.getEdgeProjectIterator(keys, pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        return this.proxy.getAggResult(pushdown);
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        return this.proxy.getAggResult(keys, pushdown);
    }

    @Override
    public void flush() {
        this.proxy.flush();
    }

    @Override
    public void drop() {
        this.client.dropDatabase(paimonStoreName);
    }

    @Override
    public void close() {
        this.proxy.close();
        this.client.close();
    }

    private void checkOrderField(List<EdgeAtom> orderFields) {
        boolean emptyFields = orderFields == null || orderFields.isEmpty();
        boolean checkOk = emptyFields || sortAtom == orderFields.get(0);
        if (!checkOk) {
            throw new GeaflowRuntimeException(
                String.format("store is sort by %s but need %s", sortAtom, orderFields.get(0)));
        }
    }
}
