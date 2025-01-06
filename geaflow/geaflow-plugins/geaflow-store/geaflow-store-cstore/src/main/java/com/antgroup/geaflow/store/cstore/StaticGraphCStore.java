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
import com.antgroup.geaflow.state.pushdown.inner.PushDownPbGenerator;
import com.antgroup.geaflow.store.api.graph.BaseGraphStore;
import com.antgroup.geaflow.store.api.graph.IStaticGraphStore;
import com.antgroup.geaflow.store.config.StoreConfigKeys;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.cstore.encoder.EdgeEncoder;
import com.antgroup.geaflow.store.cstore.encoder.EncoderFactory;
import com.antgroup.geaflow.store.cstore.encoder.VertexEncoder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A static graph store implementation using CStore as the underlying storage engine.
 * This class provides functionality for storing and retrieving vertices and edges in a graph,
 * supporting various operations like adding vertices/edges, querying graph components,
 * and iterating over graph elements.
 *
 * @param <K> The type of the vertex/edge ID
 * @param <VV> The type of the vertex value
 * @param <EV> The type of the edge value
 */
public class StaticGraphCStore<K, VV, EV> extends BaseGraphStore implements IStaticGraphStore<K, VV, EV> {

    private Map<String, String> config;
    private NativeGraphStore nativeGraphStore;
    private VertexEncoder vertexEncoder;
    private EdgeEncoder edgeEncoder;
    private IType keyType;

    /**
     * Initializes the graph store with the given store context.
     * Sets up necessary encoders and configurations for the underlying native store.
     *
     * @param storeContext The context containing configuration and schema information
     */
    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.config = storeContext.getConfig().getConfigMap();
        rewriteConfig();
        String name = storeContext.getName();
        this.keyType = storeContext.getGraphSchema().getKeyType();
        this.nativeGraphStore = new NativeGraphStore(name, storeContext.getShardId(), config);
        this.vertexEncoder = EncoderFactory.getVertexEncoder(storeContext.getGraphSchema());
        this.edgeEncoder = EncoderFactory.getEdgeEncoder(storeContext.getGraphSchema());
    }

    /**
     * Archives the graph store to a checkpoint.
     *
     * @param checkpointId The ID of the checkpoint
     */
    @Override
    public void archive(long checkpointId) {
        this.nativeGraphStore.archive(checkpointId);
    }

    /**
     * Recovers the graph store from a checkpoint.
     *
     * @param checkpointId The ID of the checkpoint
     */
    @Override
    public void recovery(long checkpointId) {
        this.nativeGraphStore.recover(checkpointId);
    }

    /**
     * Recovers the graph store to the latest checkpoint.
     *
     * @return The ID of the latest checkpoint
     */
    @Override
    public long recoveryLatest() {
        throw new UnsupportedOperationException();
    }

    /**
     * Compacts the graph store to reduce storage usage.
     */
    @Override
    public void compact() {
        this.nativeGraphStore.compact();
    }

    /**
     * Flushes the graph store to ensure data persistence.
     */
    @Override
    public void flush() {
        this.nativeGraphStore.flush();
    }

    /**
     * Closes the graph store and releases resources.
     */
    @Override
    public void close() {
        this.nativeGraphStore.close();
    }

    /**
     * Drops the graph store and releases resources.
     */
    @Override
    public void drop() {
        this.nativeGraphStore.close();
    }

    /**
     * Adds an edge to the graph store.
     * The edge is encoded before being stored in the native graph store.
     *
     * @param edge The edge to be added
     */
    @Override
    public void addEdge(IEdge<K, EV> edge) {
        this.nativeGraphStore.addEdge(this.edgeEncoder.encode(edge));
    }

    /**
     * Adds a vertex to the graph store.
     * The vertex is encoded before being stored in the native graph store.
     *
     * @param vertex The vertex to be added
     */
    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        this.nativeGraphStore.addVertex(this.vertexEncoder.encode(vertex));
    }

    /**
     * Retrieves a vertex by its ID with optional push down filters.
     *
     * @param sid The ID of the vertex to retrieve
     * @param pushdown Optional filters to apply during retrieval
     * @return The vertex if found, null otherwise
     */
    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        CStoreVertexIterator iterator =
            pushdown.getFilter() == null || pushdown.getFilter().getFilterType() == FilterType.EMPTY
            ? this.nativeGraphStore.getVertex(serializeKey(sid))
            : this.nativeGraphStore.getVertex(serializeKey(sid), getPushDownFilter(pushdown));
        return getVertexFromIterator(iterator);
    }

    /**
     * Retrieves all edges connected to a vertex with optional push down filters.
     *
     * @param sid The ID of the vertex whose edges to retrieve
     * @param pushdown Optional filters to apply during retrieval
     * @return List of edges connected to the vertex
     */
    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        List<IEdge<K, EV>> edges = new ArrayList<>();
        try (CStoreEdgeIterator iterator = this.nativeGraphStore.getEdges(serializeKey(sid),
            getPushDownFilter(pushdown))) {
            while (iterator.hasNext()) {
                edges.add(this.edgeEncoder.decode(iterator.next()));
            }
        }
        return edges;
    }

    /**
     * Retrieves a one-degree graph (vertex and its connected edges) for a given vertex ID.
     *
     * @param sid The ID of the vertex
     * @param pushdown Optional filters to apply during retrieval
     * @return OneDegreeGraph containing the vertex and its edges
     */
    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        VertexAndEdge vertexAndEdge = this.nativeGraphStore.getVertexAndEdge(serializeKey(sid),
            getPushDownFilter(pushdown));
        return new OneDegreeGraph<>(sid, getVertexFromIterator(vertexAndEdge.vertexIter),
            convertIterator(vertexAndEdge.edgeIter));
    }

    /**
     * Returns an iterator over vertex IDs in the graph store.
     *
     * @return CloseableIterator over vertex IDs
     */
    @Override
    public CloseableIterator<K> vertexIDIterator() {
        return new IteratorWithFn<>(convertIterator(this.nativeGraphStore.scanVertex()), IVertex::getId);
    }

    /**
     * Returns an iterator over vertex IDs in the graph store with optional push down filters.
     *
     * @param pushDown Optional filters to apply during iteration
     * @return CloseableIterator over vertex IDs
     */
    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown) {
        return new IteratorWithFn<>(getVertexIterator(pushDown), IVertex::getId);
    }

    /**
     * Returns an iterator over vertices in the graph store with optional push down filters.
     *
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over vertices
     */
    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        CStoreVertexIterator iterator = pushdown.isEmpty() ? this.nativeGraphStore.scanVertex()
                                                           : this.nativeGraphStore.scanVertex(
                                                               getPushDownFilter(pushdown));
        return convertIterator(iterator);
    }

    /**
     * Returns an iterator over vertices in the graph store for a list of vertex IDs with optional push down filters.
     *
     * @param keys List of vertex IDs
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over vertices
     */
    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return convertIterator(this.nativeGraphStore.scanVertex(serializeKeys(keys), getPushDownFilter(pushdown)));
    }

    /**
     * Returns an iterator over edges in the graph store with optional push down filters.
     *
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over edges
     */
    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        return convertIterator(this.nativeGraphStore.scanEdges(getPushDownFilter(pushdown)));
    }

    /**
     * Returns an iterator over edges in the graph store for a list of vertex IDs with optional push down filters.
     *
     * @param keys List of vertex IDs
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over edges
     */
    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return convertIterator(this.nativeGraphStore.scanEdges(serializeKeys(keys), getPushDownFilter(pushdown)));
    }

    /**
     * Returns an iterator over one-degree graphs (vertex and its connected edges) in the graph store with optional push down filters.
     *
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over one-degree graphs
     */
    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(IStatePushDown pushdown) {
        return convertIterator(this.nativeGraphStore.scanVertexAndEdge(getPushDownFilter(pushdown)));
    }

    /**
     * Returns an iterator over one-degree graphs (vertex and its connected edges) in the graph store for a list of vertex IDs with optional push down filters.
     *
     * @param keys List of vertex IDs
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over one-degree graphs
     */
    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys,
                                                                                  IStatePushDown pushdown) {
        return convertIterator(
            this.nativeGraphStore.scanVertexAndEdge(serializeKeys(keys), getPushDownFilter(pushdown)));
    }

    /**
     * Returns an iterator over projected edges in the graph store with optional push down filters.
     *
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over projected edges
     */
    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns an iterator over projected edges in the graph store for a list of vertex IDs with optional push down filters.
     *
     * @param keys List of vertex IDs
     * @param pushdown Optional filters to apply during iteration
     * @return CloseableIterator over projected edges
     */
    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the aggregated result of a push down operation on the graph store.
     *
     * @param pushdown Optional filters to apply during aggregation
     * @return Map of aggregated results
     */
    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the aggregated result of a push down operation on the graph store for a list of vertex IDs.
     *
     * @param keys List of vertex IDs
     * @param pushdown Optional filters to apply during aggregation
     * @return Map of aggregated results
     */
    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        throw new UnsupportedOperationException();
    }

    /**
     * Rewrites the configuration for different persistent storage types (LOCAL, DFS, OSS).
     * Sets up necessary configuration parameters based on the storage type.
     */
    private void rewriteConfig() {
        this.config.put(StoreConfigKeys.STORE_FILTER_CODEGEN_ENABLE.getKey(), "false");
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
                jsonConfig = Preconditions.checkNotNull(Configuration.getString(FileConfigKeys.JSON_CONFIG, config));
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

    /**
     * Serializes a key using the configured key type.
     *
     * @param key The key to serialize
     * @return Serialized byte array representation of the key
     */
    private byte[] serializeKey(K key) {
        return this.keyType.serialize(key);
    }

    /**
     * Serializes a list of keys using the configured key type.
     *
     * @param keys List of keys to serialize
     * @return Serialized byte array representation of the keys
     */
    private byte[][] serializeKeys(List<K> keys) {
        byte[][] serializedKeys = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            serializedKeys[i] = serializeKey(keys.get(i));
        }
        return serializedKeys;
    }

    /**
     * Returns the push down filter bytes for a given push down operation.
     *
     * @param pushdown The push down operation
     * @return Push down filter bytes
     */
    private byte[] getPushDownFilter(IStatePushDown pushdown) {
        return PushDownPbGenerator.getPushDownPbBytes(this.keyType, pushdown);
    }

    /**
     * Returns a vertex from a vertex iterator.
     *
     * @param vertexIterator The vertex iterator
     * @return The vertex if found, null otherwise
     */
    private IVertex<K, VV> getVertexFromIterator(CStoreVertexIterator vertexIterator) {
        VertexContainer container = vertexIterator.hasNext() ? vertexIterator.next() : null;
        vertexIterator.close();
        return container == null ? null : this.vertexEncoder.decode(container);
    }

    /**
     * Converts a CStore edge iterator to a CloseableIterator over edges.
     *
     * @param edgeIterator The CStore edge iterator
     * @return CloseableIterator over edges
     */
    private CloseableIterator<IEdge<K, EV>> convertIterator(CStoreEdgeIterator edgeIterator) {
        return new StandardIterator<>(new IteratorWithFn<>(edgeIterator,
            (Function<EdgeContainer, IEdge<K, EV>>) container -> edgeEncoder.decode(container)));
    }

    /**
     * Converts a CStore vertex iterator to a CloseableIterator over vertices.
     *
     * @param vertexIterator The CStore vertex iterator
     * @return CloseableIterator over vertices
     */
    private CloseableIterator<IVertex<K, VV>> convertIterator(CStoreVertexIterator vertexIterator) {
        return new StandardIterator<>(new IteratorWithFn<>(vertexIterator,
            (Function<VertexContainer, IVertex<K, VV>>) container -> vertexEncoder.decode(container)));
    }

    /**
     * Converts a CStore vertex and edge iterator to a CloseableIterator over one-degree graphs.
     *
     * @param iterator The CStore vertex and edge iterator
     * @return CloseableIterator over one-degree graphs
     */
    private CloseableIterator<OneDegreeGraph<K, VV, EV>> convertIterator(CStoreVertexAndEdgeIterator iterator) {
        return new IteratorWithFn<>(iterator, container -> {
            IVertex<K, VV> vertex = getVertexFromIterator(new CStoreVertexIterator(container.vertexIt));
            CloseableIterator<IEdge<K, EV>> edgeIter = convertIterator(new CStoreEdgeIterator(container.edgeIt));
            K key = (K) keyType.deserialize(container.sid);
            return new OneDegreeGraph<>(key, vertex, edgeIter);
        });
    }
}
