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

package com.antgroup.geaflow.dsl.runtime;

import com.antgroup.geaflow.dsl.common.data.Row;
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

/**
 * The runtime table view which  mapping SQL function to the runtime
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

    SinkDataView write(GeaFlowTable table);

    SinkDataView write(GeaFlowGraph graph, QueryContext queryContext);

    List<Row> take();

    default ViewType getType() {
        return ViewType.TABLE;
    }
}
