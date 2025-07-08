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

package com.antgroup.geaflow.dsl.runtime;

// import com.antgroup.geaflow.api.function.base.KeySelector;

import com.antgroup.geaflow.common.type.IType;

// import com.antgroup.geaflow.dsl.common.data.Path;

import com.antgroup.geaflow.dsl.common.data.Row;

// import com.antgroup.geaflow.dsl.runtime.function.graph.StepJoinFunction;

import com.antgroup.geaflow.dsl.runtime.function.table.AggFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.CorrelateFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.GroupByFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.JoinTableFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.OrderByFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.ProjectFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.WhereFunction;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import java.util.List;

// import org.apache.calcite.rel.type.RelDataType;
// import org.apache.calcite.rex.RexNode;

/**
 * The runtime table view which mapping SQL function to the runtime
 * representation of the underlying engine.
 */
public interface RuntimeTable extends RDataView {

    RuntimeTable project(ProjectFunction function);

    RuntimeTable filter(WhereFunction function);

    RuntimeTable join(RuntimeTable other, JoinTableFunction function);

    RuntimeTable aggregate(GroupByFunction groupByFunction, AggFunction aggFunction);

    RuntimeTable union(RuntimeTable other);

    RuntimeTable orderBy(OrderByFunction function);

    RuntimeTable correlate(CorrelateFunction function);

    // /**
    //  * 对两个 RuntimeTable 执行 Left Outer Join 操作.
    //  * 
    //  * @param other            右表
    //  * @param function         Join 逻辑函数
    //  * @param leftKeySelector  左表的 Key 提取器
    //  * @param rightKeySelector 右表的 Key 提取器
    //  * @return Join 后的新 RuntimeTable
    //  */
    // RuntimeTable leftOuterJoin(RuntimeTable other, JoinTableFunction function,
    //         KeySelector<Row, ?> leftKeySelector,
    //         KeySelector<Row, ?> rightKeySelector);

    // <K> RuntimeTable optionalMatch(RuntimeTable other,
    //                                KeySelector<Path, K> leftKeySelector,
    //                                KeySelector<Path, K> rightKeySelector,
    //                                StepJoinFunction joinFunction);

    SinkDataView write(GeaFlowTable table);

    SinkDataView write(GeaFlowGraph graph, QueryContext queryContext);

    List<Row> take(IType<?> type);

    default ViewType getType() {
        return ViewType.TABLE;
    }
}
