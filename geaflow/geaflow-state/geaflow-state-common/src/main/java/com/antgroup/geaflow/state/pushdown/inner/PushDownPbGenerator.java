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

package com.antgroup.geaflow.state.pushdown.inner;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.pushdown.IStatePushDown;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.EdgeLimit;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.FilterNode;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.FilterNodes;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.PushDown;
import com.antgroup.geaflow.state.pushdown.limit.IEdgeLimit;
import com.antgroup.geaflow.state.pushdown.limit.LimitType;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class PushDownPbGenerator {

    private static final byte[] EMPTY = new byte[0];

    public static PushDown getPushDownPb(IType type, IStatePushDown pushDown) {
        PushDown.Builder builder = PushDown.newBuilder();
        if (pushDown.getFilters() == null) {
            builder.setFilterNode(FilterGenerator.getFilterData(pushDown.getFilter()));
        } else {
            List<ByteString> keys = new ArrayList<>(pushDown.getFilters().size());
            List<FilterNode> filterNodes = new ArrayList<>(pushDown.getFilters().size());
            for (Object obj: pushDown.getFilters().entrySet()) {
                Entry<Object, IFilter> entry = (Entry<Object, IFilter>) obj;
                keys.add(ByteString.copyFrom(type.serialize(entry.getKey())));
                filterNodes.add(FilterGenerator.getFilterData(entry.getValue()));
            }
            FilterNodes nodes = FilterNodes.newBuilder()
                .addAllKeys(keys).addAllFilterNodes(filterNodes).build();
            builder.setFilterNodes(nodes);
        }
        IEdgeLimit limit = pushDown.getEdgeLimit();
        if (limit != null) {
            builder.setEdgeLimit(EdgeLimit.newBuilder()
                .setIn(limit.inEdgeLimit())
                .setOut(limit.outEdgeLimit())
                .setIsSingle(limit.limitType() == LimitType.SINGLE)
                .build());
        }
        if (pushDown.getOrderFields() != null) {
            List<EdgeAtom> edgeAtoms = pushDown.getOrderFields();
            builder.addAllSortType(edgeAtoms.stream().map(EdgeAtom::toPbSortType).collect(Collectors.toList()));
        }
        return builder.build();
    }

    public static byte[] getPushDownPbBytes(IType type, IStatePushDown pushDown) {
        if (pushDown.isEmpty()) {
            return EMPTY;
        }
        return getPushDownPb(type, pushDown).toByteArray();
    }
}
