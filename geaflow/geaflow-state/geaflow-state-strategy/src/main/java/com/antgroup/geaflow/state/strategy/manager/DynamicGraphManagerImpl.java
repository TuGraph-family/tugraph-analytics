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

package com.antgroup.geaflow.state.strategy.manager;

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.DynamicGraphTrait;
import com.antgroup.geaflow.state.iterator.IteratorWithFilter;
import com.antgroup.geaflow.state.iterator.MultiIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.strategy.accessor.IAccessor;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DynamicGraphManagerImpl<K, VV, EV> extends BaseShardManager<K,
    DynamicGraphTrait<K, VV, EV>> implements DynamicGraphTrait<K, VV, EV> {


    public DynamicGraphManagerImpl(StateContext context, Map<Integer, IAccessor> accessorMap) {
        super(context, accessorMap);
    }

    @Override
    public void addEdge(long version, IEdge<K, EV> edge) {
        getTraitByKey(edge.getSrcId()).addEdge(version, edge);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getEdges(version, sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid,
                                                       IStatePushDown pushdown) {
        return getTraitByKey(sid).getOneDegreeGraph(version, sid, pushdown);
    }

    @Override
    public void addVertex(long version, IVertex<K, VV> vertex) {
        getTraitByKey(vertex.getId()).addVertex(version, vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getVertex(version, sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        List<CloseableIterator<K>> iterators = new ArrayList<>();
        for (Entry<Integer, DynamicGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            CloseableIterator<K> iterator = entry.getValue().vertexIDIterator();
            iterators.add(this.mayScale ? shardFilter(iterator, entry.getKey(), k -> k) : iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushdown) {
        List<CloseableIterator<K>> iterators = new ArrayList<>();
        KeyGroup shardGroup = getShardGroup(pushdown);

        for (int shard = shardGroup.getStartKeyGroup(); shard <= shardGroup.getEndKeyGroup(); shard++) {
            DynamicGraphTrait<K, VV, EV> trait = traitMap.get(shard);
            CloseableIterator<K> iterator = trait.vertexIDIterator(version, pushdown);
            iterators.add(this.mayScale ? shardFilter(iterator, shard, k -> k) : iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown) {
        return getIterator(IVertex::getId, pushdown, (trait, pushdown1) -> trait.getVertexIterator(version, pushdown1));
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                      IStatePushDown pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getVertexIterator(version, keys1, pushdown1));
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        return getIterator(IEdge::getSrcId, pushdown, (trait, pushdown1) -> trait.getEdgeIterator(version, pushdown1));
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                  IStatePushDown pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getEdgeIterator(version, keys1, pushdown1));
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                         IStatePushDown pushdown) {
        return getIterator(OneDegreeGraph::getKey, pushdown,
            (trait, pushdown1) -> trait.getOneDegreeGraphIterator(version, pushdown1));
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys,
                                                                         IStatePushDown pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getOneDegreeGraphIterator(version, keys1, pushdown1));
    }

    private <R> CloseableIterator<R> getIterator(List<K> keys,
                                        IStatePushDown pushdown,
                                        TriFunction<DynamicGraphTrait<K, VV, EV>, List<K>, IStatePushDown, CloseableIterator<R>> function) {
        List<CloseableIterator<R>> iterators = new ArrayList<>();
        Map<Integer, List<K>> keyGroupMap = getKeyGroupMap(keys);
        for (Entry<Integer, List<K>> entry: keyGroupMap.entrySet()) {
            Preconditions.checkArgument(
                entry.getKey() >= this.shardGroup.getStartKeyGroup()
                && entry.getKey() <= this.shardGroup.getEndKeyGroup());

            CloseableIterator<R> iterator = function.apply(getTraitById(entry.getKey()), entry.getValue(), pushdown);
            iterators.add(iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }


    private <R> CloseableIterator<R> getIterator(
        Function<R, K> keyExtractor,
        IStatePushDown pushdown,
        BiFunction<DynamicGraphTrait<K, VV, EV>, IStatePushDown, CloseableIterator<R>> function) {
        List<CloseableIterator<R>> iterators = new ArrayList<>();

        KeyGroup shardGroup = getShardGroup(pushdown);
        int startShard = shardGroup.getStartKeyGroup();
        int endShard = shardGroup.getEndKeyGroup();

        for (int shard = startShard; shard <= endShard; shard++) {
            DynamicGraphTrait<K, VV, EV> trait = traitMap.get(shard);
            CloseableIterator<R> iterator = function.apply(trait, pushdown);
            iterators.add(this.mayScale ? shardFilter(iterator, shard, keyExtractor) : iterator);
        }

        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public List<Long> getAllVersions(K id, DataType dataType) {
        return getTraitByKey(id).getAllVersions(id, dataType);
    }

    @Override
    public long getLatestVersion(K id, DataType dataType) {
        return getTraitByKey(id).getLatestVersion(id, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown,
                                                       DataType dataType) {
        return getTraitByKey(id).getAllVersionData(id, pushdown, dataType);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> versions,
                                                    IStatePushDown pushdown, DataType dataType) {
        return getTraitByKey(id).getVersionData(id, versions, pushdown, dataType);
    }

    private <T> CloseableIterator<T> shardFilter(CloseableIterator<T> iterator, int keyGroupId,
                                        Function<T, K> keyExtractor) {
        return new IteratorWithFilter<>(iterator, t -> assigner.assign(keyExtractor.apply(t)) == keyGroupId);
    }

    private Map<Integer, List<K>> getKeyGroupMap(Collection<K> keySet) {
        return keySet.stream().collect(Collectors.groupingBy(c -> assigner.assign(c)));
    }
}
