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

package com.antgroup.geaflow.store.rocksdb.proxy;

import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.EDGE_CF;
import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_CF;
import static com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys.VERTEX_INDEX_CF;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.encoder.IEdgeKVEncoder;
import com.antgroup.geaflow.state.graph.encoder.IGraphKVEncoder;
import com.antgroup.geaflow.state.graph.encoder.IVertexKVEncoder;
import com.antgroup.geaflow.state.iterator.IteratorWithFlatFn;
import com.antgroup.geaflow.state.iterator.IteratorWithFnThenFilter;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.store.iterator.KeysIterator;
import com.antgroup.geaflow.store.rocksdb.RocksdbClient;
import com.antgroup.geaflow.store.rocksdb.iterator.EdgeScanIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.OneDegreeGraphScanIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.RocksdbIterator;
import com.antgroup.geaflow.store.rocksdb.iterator.VertexScanIterator;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class SyncGraphMultiVersionedProxy<K, VV, EV> implements IGraphMultiVersionedRocksdbProxy<K, VV, EV> {

    private static final int VERSION_BYTES_SIZE = Long.BYTES;
    private static final int VERTEX_INDEX_SUFFIX_SIZE =
        VERSION_BYTES_SIZE + StateConfigKeys.DELIMITER.length;
    protected static final byte[] EMPTY_BYTES = new byte[0];
    protected final Configuration config;
    protected RocksdbClient rocksdbClient;
    protected IGraphKVEncoder<K, VV, EV> encoder;
    protected IEdgeKVEncoder<K, EV> edgeEncoder;
    protected IVertexKVEncoder<K, VV> vertexEncoder;

    public SyncGraphMultiVersionedProxy(RocksdbClient rocksdbStore,
                                        IGraphKVEncoder<K, VV, EV> encoder,
                                        Configuration config) {
        this.encoder = encoder;
        this.rocksdbClient = rocksdbStore;
        this.vertexEncoder = encoder.getVertexEncoder();
        this.edgeEncoder = encoder.getEdgeEncoder();
        this.config = config;
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        Tuple<byte[], byte[]> tuple = vertexEncoder.format(vertex);
        byte[] bVersion = getBinaryVersion(version);
        this.rocksdbClient.write(VERTEX_CF, concat(bVersion, tuple.f0), tuple.f1);
        this.rocksdbClient.write(VERTEX_INDEX_CF, concat(tuple.f0, bVersion), EMPTY_BYTES);
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        byte[] bVersion = getBinaryVersion(version);
        Tuple<byte[], byte[]> tuple = edgeEncoder.format(edge);
        this.rocksdbClient.write(EDGE_CF, concat(bVersion, tuple.f0), tuple.f1);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        byte[] key = encoder.getKeyType().serialize(sid);
        byte[] bVersion = getBinaryVersion(version);
        byte[] value = this.rocksdbClient.get(VERTEX_CF, concat(bVersion, key));
        if (value != null) {
            IVertex<K, VV> vertex = vertexEncoder.getVertex(key, value);
            if (pushdown == null || ((IGraphFilter)pushdown.getFilter()).filterVertex(vertex)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        List<IEdge<K, EV>> list = new ArrayList<>();
        byte[] bVersion = getBinaryVersion(version);
        byte[] prefix = concat(bVersion, edgeEncoder.getScanBytes(sid));

        IGraphFilter filter = (IGraphFilter) pushdown.getFilter();
        try (RocksdbIterator it = new RocksdbIterator(
            this.rocksdbClient.getIterator(EDGE_CF), prefix)) {
            while (it.hasNext()) {
                Tuple<byte[], byte[]> pair = it.next();
                IEdge<K, EV> edge = edgeEncoder.getEdge(getKeyFromVersionToKey(pair.f0), pair.f1);
                if (filter.filterEdge(edge)) {
                    list.add(edge);
                }
            }
        }
        return list;
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid, IStatePushDown pushdown) {
        IVertex<K, VV> vertex = getVertex(version, sid, pushdown);
        List<IEdge<K, EV>> edgeList = getEdges(version, sid, pushdown);
        OneDegreeGraph<K, VV, EV> oneDegreeGraph = new OneDegreeGraph<>(sid, vertex, edgeList.iterator());
        if (((IGraphFilter)pushdown.getFilter()).filterOneDegreeGraph(oneDegreeGraph)) {
            return oneDegreeGraph;
        } else {
            return null;
        }
    }

    @Override
    public Iterator<K> vertexIDIterator() {
        flush();
        RocksdbIterator it = new RocksdbIterator(this.rocksdbClient.getIterator(VERTEX_INDEX_CF));

        return new IteratorWithFnThenFilter<>(it,
            tuple2 -> vertexEncoder.getVertexID(getKeyFromKeyToVersion(tuple2.f0)),
            new Predicate<K>() {
                K last = null;
                @Override
                public boolean test(K k) {
                    boolean res = k.equals(last);
                    last = k;
                    return !res;
                }
            });
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown) {
        flush();
        byte[] prefix = getVersionPrefix(version);
        RocksdbIterator it = new RocksdbIterator(rocksdbClient.getIterator(VERTEX_CF), prefix);
        return new VertexScanIterator<>(it, pushdown,
            (key, value) -> vertexEncoder.getVertex(getKeyFromVersionToKey(key), value));
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                      IStatePushDown pushdown) {
        return new KeysIterator<>(keys, (k, f) -> getVertex(version, k, f), pushdown);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        flush();
        byte[] prefix = getVersionPrefix(version);
        RocksdbIterator it = new RocksdbIterator(rocksdbClient.getIterator(EDGE_CF), prefix);
        return new EdgeScanIterator<>(it, pushdown,
            (key, value) -> edgeEncoder.getEdge(getKeyFromVersionToKey(key), value));
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                  IStatePushDown pushdown) {
        return new IteratorWithFlatFn<>(new KeysIterator<>(keys, (k, f) -> getEdges(version, k, f), pushdown), List::iterator);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                         IStatePushDown pushdown) {
        flush();
        return new OneDegreeGraphScanIterator<>(
            encoder.getKeyType(),
            getVertexIterator(version, pushdown),
            getEdgeIterator(version, pushdown),
            pushdown);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys,
                                                                         IStatePushDown pushdown) {
        return new KeysIterator<>(keys, (k, f) -> getOneDegreeGraph(version, k, f), pushdown);
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        flush();
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            List<Long> list = new ArrayList<>();
            byte[] prefix = Bytes.concat(encoder.getKeyType().serialize(id), StateConfigKeys.DELIMITER);
            try (RocksdbIterator it =
                new RocksdbIterator(this.rocksdbClient.getIterator(VERTEX_INDEX_CF), prefix)) {
                while (it.hasNext()) {
                    Tuple<byte[], byte[]> pair = it.next();
                    list.add(getVersionFromKeyToVersion(pair.f0));
                }
            }
            return list;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        flush();
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            byte[] prefix = getKeyPrefix(id);
            try (RocksdbIterator it =
                new RocksdbIterator(this.rocksdbClient.getIterator(VERTEX_INDEX_CF), prefix)) {
                if (it.hasNext()) {
                    Tuple<byte[], byte[]> pair = it.next();
                    return getVersionFromKeyToVersion(pair.f0);
                }
            }
            return -1;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        List<Long> allVersions = getAllVersions(id, dataType);
        return getVersionData(id, allVersions, pushdown, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> versions,
                                                    IStatePushDown pushdown, DataType dataType) {
        if (dataType == DataType.V || dataType == DataType.V_TOPO) {
            Map<Long, IVertex<K, VV>> map = new HashMap<>();
            for (long version: versions) {
                IVertex<K, VV> vertex = getVertex(version, id, pushdown);
                if (vertex != null) {
                    map.put(version, vertex);
                }
            }
            return map;
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }


    @Override
    public RocksdbClient getClient() {
        return rocksdbClient;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    private long getVersionFromKeyToVersion(byte[] key) {
        byte[] bVersion = Arrays.copyOfRange(key, key.length - 8, key.length);
        return Long.MAX_VALUE - Longs.fromByteArray(bVersion);
    }

    protected byte[] getKeyFromKeyToVersion(byte[] key) {
        return Arrays.copyOf(key, key.length - VERTEX_INDEX_SUFFIX_SIZE);
    }

    protected byte[] getBinaryVersion(long version) {
        return Longs.toByteArray(Long.MAX_VALUE - version);
    }

    protected byte[] getKeyPrefix(K id) {
        return Bytes.concat(this.encoder.getKeyType().serialize(id), StateConfigKeys.DELIMITER);
    }

    protected byte[] getVersionPrefix(long version) {
        return Bytes.concat(getBinaryVersion(version), StateConfigKeys.DELIMITER);
    }

    protected byte[] getKeyFromVersionToKey(byte[] key) {
        return Arrays.copyOfRange(key, 10, key.length);
    }

    protected byte[] concat(byte[] a, byte[] b) {
        return Bytes.concat(a, StateConfigKeys.DELIMITER, b);
    }
}
