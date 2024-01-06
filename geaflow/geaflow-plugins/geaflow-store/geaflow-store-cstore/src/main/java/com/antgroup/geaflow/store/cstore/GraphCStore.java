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

package com.antgroup.geaflow.store.cstore;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.PersistentType;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.iterator.IteratorWithFn;
import com.antgroup.geaflow.state.iterator.StandardIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.FilterType;
import com.antgroup.geaflow.state.pushdown.inner.IFilterConverter;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPbGenerator;
import com.antgroup.geaflow.store.api.graph.IGraphStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.cstore.encoder.EdgeEncoder;
import com.antgroup.geaflow.store.cstore.encoder.EncoderFactory;
import com.antgroup.geaflow.store.cstore.encoder.VertexEncoder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GraphCStore<K, VV, EV> implements IGraphStore<K, VV, EV> {

    private IFilterConverter filterConverter = new FilterConverter();
    private Map<String, String> config;
    private String name;
    private NativeGraphStore nativeGraphStore;
    private VertexEncoder vertexEncoder;
    private EdgeEncoder edgeEncoder;
    private IType keyType;

    @Override
    public void init(StoreContext storeContext) {
        this.config = storeContext.getConfig().getConfigMap();
        rewriteConfig();
        this.name = storeContext.getName();
        this.keyType = storeContext.getGraphSchema().getKeyType();
        this.nativeGraphStore = new NativeGraphStore(name, storeContext.getShardId(), config);
        this.vertexEncoder = EncoderFactory.getVertexEncoder(storeContext.getGraphSchema());
        this.edgeEncoder = EncoderFactory.getEdgeEncoder(storeContext.getGraphSchema());
    }

    @Override
    public void archive(long checkpointId) {
        this.nativeGraphStore.archive(checkpointId);
    }

    @Override
    public void recovery(long checkpointId) {
        this.nativeGraphStore.recover(checkpointId);
    }

    @Override
    public long recoveryLatest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact() {
        this.nativeGraphStore.compact();
    }

    @Override
    public void flush() {
        this.nativeGraphStore.flush();
    }

    @Override
    public void close() {
        this.nativeGraphStore.close();
    }

    @Override
    public void drop() {
        this.nativeGraphStore.close();
        // TODO: drop
    }

    private void rewriteConfig() {
        String jobName = Configuration.getString(ExecutionConfigKeys.JOB_APP_NAME, this.config);
        String workerPath = Configuration.getString(ExecutionConfigKeys.JOB_WORK_PATH, this.config);
        this.config.put(CStoreConfigKeys.CSTORE_NAME_KEY, jobName);
        this.config.put(CStoreConfigKeys.CSTORE_LOCAL_ROOT, workerPath);
        PersistentType type = PersistentType.valueOf(
            Configuration.getString(FileConfigKeys.PERSISTENT_TYPE, this.config).toUpperCase());
        String root;
        this.config.put(CStoreConfigKeys.CSTORE_PERSISTENT_TYPE, type.name());
        switch (type) {
            case LOCAL:
                if (this.config.containsKey(FileConfigKeys.ROOT.getKey())) {
                    root = Configuration.getString(FileConfigKeys.ROOT, this.config);
                } else {
                    root = workerPath + Configuration.getString(FileConfigKeys.ROOT, this.config);
                }
                this.config.put(CStoreConfigKeys.CSTORE_PERSISTENT_ROOT, root);
                break;
            case DFS:
                String jsonConfig = Preconditions.checkNotNull(
                    Configuration.getString(FileConfigKeys.JSON_CONFIG, this.config));
                root = Configuration.getString(FileConfigKeys.ROOT, this.config);
                this.config.put(CStoreConfigKeys.CSTORE_PERSISTENT_ROOT, root);
                Map<String, String> persistConfig = GsonUtil.parse(jsonConfig);
                String fsUrl = persistConfig.get("fs.defaultFS");
                if (fsUrl.startsWith("hdfs")) {
                    this.config.put(CStoreConfigKeys.CSTORE_PERSISTENT_TYPE, "hdfs");
                }
                this.config.put(CStoreConfigKeys.CSTORE_DFS_CLUSTER, fsUrl);
                break;
            case OSS:
                jsonConfig = Preconditions.checkNotNull(
                    Configuration.getString(FileConfigKeys.JSON_CONFIG, config));
                persistConfig = GsonUtil.parse(jsonConfig);

                root = Configuration.getString(FileConfigKeys.ROOT, this.config);
                this.config.put(CStoreConfigKeys.CSTORE_PERSISTENT_ROOT, root);
                String bucketName = Configuration.getString(FileConfigKeys.OSS_BUCKET_NAME, persistConfig);
                this.config.put(CStoreConfigKeys.CSTORE_OSS_BUCKET, bucketName);
                String endpoint = Configuration.getString(FileConfigKeys.OSS_ENDPOINT, persistConfig);
                this.config.put(CStoreConfigKeys.CSTORE_OSS_ENDPOINT, endpoint);
                String accessKeyId = Configuration.getString(FileConfigKeys.OSS_ACCESS_ID, persistConfig);
                this.config.put(CStoreConfigKeys.CSTORE_OSS_AK, accessKeyId);
                String accessKeySecret = Configuration.getString(FileConfigKeys.OSS_SECRET_KEY, persistConfig);
                this.config.put(CStoreConfigKeys.CSTORE_OSS_SK, accessKeySecret);
                break;
            default:
                throw new UnsupportedOperationException("not support type " + type);
        }
    }

    @Override
    public IFilterConverter getFilterConverter() {
        return filterConverter;
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        EdgeContainer container = this.edgeEncoder.encode(edge);
        this.nativeGraphStore.addEdge(container);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        VertexContainer container = this.vertexEncoder.encode(vertex);
        this.nativeGraphStore.addVertex(container);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        CStoreVertexIterator iterator;
        if (pushdown.getFilter() == null
            || pushdown.getFilter().getFilterType() == FilterType.EMPTY) {
            iterator = this.nativeGraphStore.getVertex(this.keyType.serialize(sid));
        } else {
            iterator = this.nativeGraphStore.getVertex(this.keyType.serialize(sid),
                PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown));
        }
        return getVertexFromIterator(iterator);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {

        byte[] key = this.keyType.serialize(sid);
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);

        List<IEdge<K, EV>> list = new ArrayList<>();
        try (CStoreEdgeIterator iterator = this.nativeGraphStore.getEdges(key, filter)) {
            while (iterator.hasNext()) {
                list.add(this.edgeEncoder.decode(iterator.next()));
            }
        }
        return list;
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        byte[] key = this.keyType.serialize(sid);
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);

        VertexAndEdge vertexAndEdge = this.nativeGraphStore.getVertexAndEdge(key, filter);
        return new OneDegreeGraph<>(sid, getVertexFromIterator(vertexAndEdge.vertexIter),
            convertIterator(vertexAndEdge.edgeIter));
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        CStoreVertexIterator iterator = this.nativeGraphStore.scanVertex();
        return new IteratorWithFn<>(convertIterator(iterator), IVertex::getId);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        return new IteratorWithFn<>(getVertexIterator(pushDown), IVertex::getId);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        CStoreVertexIterator iterator;
        if (pushdown.isEmpty()) {
            iterator = this.nativeGraphStore.scanVertex();
        } else {
            byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
            iterator = this.nativeGraphStore.scanVertex(filter);
        }
        return convertIterator(iterator);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys,
                                                               IStatePushDown pushdown) {
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
        byte[][] scanKeys = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            scanKeys[i] = this.keyType.serialize(keys.get(i));
        }
        CStoreVertexIterator iterator = this.nativeGraphStore.scanVertex(scanKeys, filter);
        return convertIterator(iterator);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
        CStoreEdgeIterator iterator = this.nativeGraphStore.scanEdges(filter);
        return convertIterator(iterator);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
        byte[][] scanKeys = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            scanKeys[i] = this.keyType.serialize(keys.get(i));
        }
        CStoreEdgeIterator iterator = this.nativeGraphStore.scanEdges(scanKeys, filter);
        return convertIterator(iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
        CStoreVertexAndEdgeIterator iterator = this.nativeGraphStore.scanVertexAndEdge(filter);
        return convertIterator(iterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys,
                                                                                  IStatePushDown pushdown) {
        byte[] filter = PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
        byte[][] scanKeys = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            scanKeys[i] = this.keyType.serialize(keys.get(i));
        }
        CStoreVertexAndEdgeIterator iterator = this.nativeGraphStore.scanVertexAndEdge(scanKeys,
            filter);
        return convertIterator(iterator);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K,
                                                                         EV>, R> pushdown) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        throw new UnsupportedOperationException();
    }

    private IVertex<K, VV> getVertexFromIterator(CStoreVertexIterator vertexIterator) {
        VertexContainer container = vertexIterator.hasNext() ? vertexIterator.next() : null;
        vertexIterator.close();
        return container == null ? null : this.vertexEncoder.decode(container);
    }

    private CloseableIterator<IEdge<K, EV>> convertIterator(CStoreEdgeIterator edgeIterator) {
        return new StandardIterator<>(new IteratorWithFn<>(edgeIterator,
            (Function<EdgeContainer, IEdge<K, EV>>) container -> edgeEncoder.decode(container)));
    }

    private CloseableIterator<IVertex<K, VV>> convertIterator(CStoreVertexIterator vertexIterator) {
        return new StandardIterator<>(new IteratorWithFn<>(vertexIterator,
            (Function<VertexContainer, IVertex<K, VV>>) container -> vertexEncoder.decode(
                container)));
    }

    private CloseableIterator<OneDegreeGraph<K, VV, EV>> convertIterator(
        CStoreVertexAndEdgeIterator iterator) {
        return new IteratorWithFn<>(iterator, container -> {
            IVertex<K, VV> vertex = getVertexFromIterator(
                new CStoreVertexIterator(container.vertexIt));
            CloseableIterator<IEdge<K, EV>> edgeIter = convertIterator(
                new CStoreEdgeIterator(container.edgeIt));
            K key = (K) keyType.deserialize(container.sid);
            return new OneDegreeGraph<>(key, vertex, edgeIter);
        });
    }
}
