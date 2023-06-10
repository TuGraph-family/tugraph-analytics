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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
    public Iterator<K> vertexIDIterator() {
        List<Iterator<K>> iterators = new ArrayList<>();
        for (Entry<Integer, DynamicGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            Iterator<K> iterator = entry.getValue().vertexIDIterator();
            iterators.add(this.mayScale ? shardFilter(iterator, entry.getKey(), k -> k) : iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown) {
        return getIterator(IVertex::getId, pushdown, (trait, pushdown1) -> trait.getVertexIterator(version, pushdown1));
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys,
                                                      IStatePushDown pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getVertexIterator(version, keys1, pushdown1));
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown) {
        return getIterator(IEdge::getSrcId, pushdown, (trait, pushdown1) -> trait.getEdgeIterator(version, pushdown1));
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys,
                                                  IStatePushDown pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getEdgeIterator(version, keys1, pushdown1));
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version,
                                                                         IStatePushDown pushdown) {
        return getIterator(OneDegreeGraph::getKey, pushdown,
            (trait, pushdown1) -> trait.getOneDegreeGraphIterator(version, pushdown1));
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys,
                                                                         IStatePushDown pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getOneDegreeGraphIterator(version, keys1, pushdown1));
    }

    private <R> Iterator<R> getIterator(List<K> keys,
                                        IStatePushDown pushdown,
                                        TriFunction<DynamicGraphTrait<K, VV, EV>, List<K>, IStatePushDown, Iterator<R>> function) {
        List<Iterator<R>> iterators = new ArrayList<>();
        Map<Integer, List<K>> keyGroupMap = getKeyGroupMap(keys);
        for (Entry<Integer, List<K>> entry: keyGroupMap.entrySet()) {
            Preconditions.checkArgument(
                entry.getKey() >= this.shardGroup.getStartKeyGroup()
                && entry.getKey() <= this.shardGroup.getEndKeyGroup());

            Iterator<R> iterator = function.apply(getTraitById(entry.getKey()), entry.getValue(), pushdown);
            iterators.add(iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }


    private <R> Iterator<R> getIterator(
        Function<R, K> keyExtractor,
        IStatePushDown pushdown,
        BiFunction<DynamicGraphTrait<K, VV, EV>, IStatePushDown, Iterator<R>> function) {
        List<Iterator<R>> iterators = new ArrayList<>();

        int startShard = this.shardGroup.getStartKeyGroup();
        int endShard = this.shardGroup.getEndKeyGroup();
        for (Entry<Integer, DynamicGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            if (entry.getKey() >= startShard && entry.getKey() <= endShard) {
                Iterator<R> iterator = function.apply(entry.getValue(), pushdown);
                iterators.add(this.mayScale ? shardFilter(iterator, entry.getKey(), keyExtractor) : iterator);
            }
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

    private <T> Iterator<T> shardFilter(Iterator<T> iterator, int keyGroupId,
                                        Function<T, K> keyExtractor) {
        return new IteratorWithFilter<>(iterator, t -> assigner.assign(keyExtractor.apply(t)) == keyGroupId);
    }

    private Map<Integer, List<K>> getKeyGroupMap(Collection<K> keySet) {
        return keySet.stream().collect(Collectors.groupingBy(c -> assigner.assign(c)));
    }
}
