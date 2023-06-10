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

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.graph.StaticGraphTrait;
import com.antgroup.geaflow.state.iterator.IteratorWithFilter;
import com.antgroup.geaflow.state.iterator.MultiIterator;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.strategy.accessor.IAccessor;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StaticGraphManagerImpl<K, VV, EV> extends BaseShardManager<K,
    StaticGraphTrait<K, VV, EV>> implements StaticGraphTrait<K, VV, EV> {


    public StaticGraphManagerImpl(StateContext context, Map<Integer, IAccessor> accessorMap) {
        super(context, accessorMap);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        getTraitByKey(edge.getSrcId()).addEdge(edge);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        getTraitByKey(vertex.getId()).addVertex(vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getVertex(sid, pushdown);
    }

    @Override
    public Iterator<K> vertexIDIterator() {
        List<Iterator<K>> iterators = new ArrayList<>();
        for (Entry<Integer, StaticGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            Iterator<K> iterator = entry.getValue().vertexIDIterator();
            iterators.add(this.mayScale ? shardFilter(iterator, entry.getKey(), k -> k) : iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return getIterator(IVertex::getId, pushdown, StaticGraphTrait::getVertexIterator);
    }

    @Override
    public Iterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return getIterator(keys, pushdown, StaticGraphTrait::getVertexIterator);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        return getIterator(IEdge::getSrcId, pushdown, StaticGraphTrait::getEdgeIterator);
    }

    @Override
    public Iterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return getIterator(keys, pushdown, StaticGraphTrait::getEdgeIterator);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        return getIterator(OneDegreeGraph::getKey, pushdown, StaticGraphTrait::getOneDegreeGraphIterator);
    }

    @Override
    public Iterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return getIterator(keys, pushdown, StaticGraphTrait::getOneDegreeGraphIterator);
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return getIterator(Tuple::getF0, pushdown, (trait, pushdown1) -> (Iterator<Tuple<K, R>>) trait.getEdgeProjectIterator(pushdown1));
    }

    @Override
    public <R> Iterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                            IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getEdgeProjectIterator(keys1, pushdown1));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<Integer, Map<K, Long>> map = new HashMap<>();
        for (Entry<Integer, StaticGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getAggResult(pushdown));
        }
        Map<K, Long> res = new HashMap<>();
        for (Entry<Integer, Map<K, Long>> partRes : map.entrySet()) {
            int keyGroupId = partRes.getKey();
            for (Entry<K, Long> entry: partRes.getValue().entrySet()) {
                if (keyGroupId != assigner.assign(entry.getKey())) {
                    continue;
                }
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Map<Integer, List<K>> keyGroupMap = getKeyGroupMap(keys);
        for (Entry<Integer, List<K>> entry: keyGroupMap.entrySet()) {
            Preconditions.checkArgument(entry.getKey() >= this.shardGroup.getStartKeyGroup()
                && entry.getKey() <= this.shardGroup.getEndKeyGroup());

            res.putAll(getTraitById(entry.getKey()).getAggResult(entry.getValue(), pushdown));
        }
        return res;
    }

    private <R> Iterator<R> getIterator(List<K> keys,
                                        IStatePushDown pushdown,
                                        TriFunction<StaticGraphTrait<K, VV, EV>, List<K>, IStatePushDown, Iterator<R>> function) {
        List<Iterator<R>> iterators = new ArrayList<>();
        Map<Integer, List<K>> keyGroupMap = getKeyGroupMap(keys);
        for (Entry<Integer, List<K>> entry: keyGroupMap.entrySet()) {
            Preconditions.checkArgument(entry.getKey() >= this.shardGroup.getStartKeyGroup()
                && entry.getKey() <= this.shardGroup.getEndKeyGroup());

            Iterator<R> iterator = function.apply(getTraitById(entry.getKey()), entry.getValue(), pushdown);
            iterators.add(iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }


    private <R> Iterator<R> getIterator(
        Function<R, K> keyExtractor,
        IStatePushDown pushdown,
        BiFunction<StaticGraphTrait<K, VV, EV>, IStatePushDown, Iterator<R>> function) {
        List<Iterator<R>> iterators = new ArrayList<>();

        int startShard = this.shardGroup.getStartKeyGroup();
        int endShard = this.shardGroup.getEndKeyGroup();
        for (Entry<Integer, StaticGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            if (entry.getKey() >= startShard && entry.getKey() <= endShard) {
                Iterator<R> iterator = function.apply(entry.getValue(), pushdown);
                iterators.add(this.mayScale ? shardFilter(iterator, entry.getKey(), keyExtractor) : iterator);
            }
        }

        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    private <T> Iterator<T> shardFilter(Iterator<T> iterator, int keyGroupId,
                                          Function<T, K> keyExtractor) {
        return new IteratorWithFilter<>(iterator, t -> assigner.assign(keyExtractor.apply(t)) == keyGroupId);
    }

    private Map<Integer, List<K>> getKeyGroupMap(Collection<K> keySet) {
        return keySet.stream().collect(Collectors.groupingBy(c -> assigner.assign(c)));
    }
}
