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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.api.function.internal.CollectionSource;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.utils.ClassUtil;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.pushdown.EnablePartitionPushDown;
import com.antgroup.geaflow.dsl.common.pushdown.PartitionFilter;
import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.ISkipOpenAndClose;
import com.antgroup.geaflow.dsl.connector.api.TableConnector;
import com.antgroup.geaflow.dsl.connector.api.TableReadableConnector;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.function.GeaFlowTableSourceFunction;
import com.antgroup.geaflow.dsl.connector.api.util.ConnectorFactory;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.QueryEngine;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.field.FieldExpression;
import com.antgroup.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import com.antgroup.geaflow.dsl.runtime.expression.logic.AndExpression;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicRelNode.PhysicRelNodeName;
import com.antgroup.geaflow.dsl.runtime.util.SchemaUtil;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * GeaFlow's implement of the {@link QueryEngine}.
 */
public class GeaFlowQueryEngine implements QueryEngine {

    private final IPipelineTaskContext pipelineContext;

    public GeaFlowQueryEngine(IPipelineTaskContext pipelineContext) {
        this.pipelineContext = pipelineContext;
    }

    @Override
    public Map<String, String> getConfig() {
        return pipelineContext.getConfig().getConfigMap();
    }

    @Override
    public IPipelineTaskContext getContext() {
        return pipelineContext;
    }

    @Override
    public RuntimeTable createRuntimeTable(QueryContext context, GeaFlowTable table, Expression pushFiler) {
        String tableType = table.getTableType();
        TableConnector connector = ConnectorFactory.loadConnector(tableType);
        if (!(connector instanceof TableReadableConnector)) {
            throw new GeaFlowDSLException("Table: '{}' is not readable", connector.getType());
        }
        TableReadableConnector readableConnector = (TableReadableConnector) connector;
        Configuration conf = table.getConfigWithGlobal(pipelineContext.getConfig(), context.getSetOptions());
        TableSource tableSource = readableConnector.createSource(conf);
        // push down filter to table source
        if (pushFiler != null) {
            List<Expression> filters = pushFiler.splitByAnd();
            List<Expression> partitionFilters = filters.stream()
                .filter(filter -> isPartitionFilter(filter, table))
                .collect(Collectors.toList());

            Expression partitionFilterExp =
                partitionFilters.isEmpty() ? LiteralExpression.createBoolean(true) :
                new AndExpression(partitionFilters);
            PartitionFilter partitionFilter = new PartitionFilterImpl(partitionFilterExp,
                table.getPartitionIndices());
            if (tableSource instanceof EnablePartitionPushDown) {
                ((EnablePartitionPushDown) tableSource).setPartitionFilter(partitionFilter);
            }
        }

        tableSource.init(conf, table.getTableSchema());

        if (!(tableSource instanceof ISkipOpenAndClose)) {
            tableSource.open(new DefaultRuntimeContext(pipelineContext.getConfig()));
        }
        int numPartitions = tableSource.listPartitions().size();
        int partitionsPerParallelism = conf.getInteger(
            ConnectorConfigKeys.GEAFLOW_DSL_PARTITIONS_PER_SOURCE_PARALLELISM);
        int sourceParallelism;
        if (numPartitions % partitionsPerParallelism > 0) {
            sourceParallelism = numPartitions / partitionsPerParallelism + 1;
        } else {
            sourceParallelism = numPartitions / partitionsPerParallelism;
        }
        if (!(tableSource instanceof ISkipOpenAndClose)) {
            tableSource.close();
        }
        String opName = PhysicRelNodeName.TABLE_SCAN.getName(table.getName());
        int parallelism = context.getConfigParallelisms(opName, sourceParallelism);
        GeaFlowTableSourceFunction sourceFunction;
        if (conf.contains(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION)) {
            String customClassName = conf.getString(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION);
            try {
                sourceFunction = (GeaFlowTableSourceFunction) ClassUtil.classForName(customClassName)
                    .getConstructor(GeaFlowTable.class, TableSource.class)
                    .newInstance(table, tableSource);
            } catch (Exception e) {
                throw new GeaFlowDSLException("Cannot create sink function: {}.",
                    customClassName, e);
            }
        } else {
            sourceFunction = new GeaFlowTableSourceFunction(table, tableSource);
        }

        IWindow<Row> window = Windows.createWindow(conf);
        PWindowSource<Row> source = pipelineContext.buildSource(sourceFunction, window)
            .withConfig(context.getSetOptions())
            .withName(opName)
            .withParallelism(parallelism);

        return new GeaFlowRuntimeTable(context, pipelineContext, source);
    }

    @Override
    public RuntimeTable createRuntimeTable(QueryContext context, Collection<Row> rows) {
        IWindow<Row> window = Windows.createWindow(pipelineContext.getConfig());
        PWindowSource<Row> source = pipelineContext.buildSource(new CollectionSource<>(rows), window)
            .withConfig(context.getSetOptions())
            .withName(PhysicRelNodeName.VALUE_SCAN.getName(context.getOpNameCount()))
            .withParallelism(1);
        return new GeaFlowRuntimeTable(context, pipelineContext, source);
    }

    @Override
    public <T> PWindowSource<T> createRuntimeTable(QueryContext context, SourceFunction<T> sourceFunction) {
        IWindow<T> window = Windows.createWindow(pipelineContext.getConfig());
        return pipelineContext.buildSource(sourceFunction, window)
            .withConfig(context.getSetOptions());
    }

    @SuppressWarnings("unchecked")
    @Override
    public RuntimeGraph createRuntimeGraph(QueryContext context, GeaFlowGraph graph) {
        GraphViewDesc graphViewDesc = SchemaUtil.buildGraphViewDesc(graph, context.getGlobalConf());
        PGraphView<Object, Row, Row> pGraphView = pipelineContext.createGraphView(graphViewDesc);
        return new GeaFlowRuntimeGraph(context, pGraphView, graph, graphViewDesc);
    }

    public IPipelineTaskContext getPipelineContext() {
        return pipelineContext;
    }

    private boolean isPartitionFilter(Expression filter, GeaFlowTable table) {
        if (filter.getInputs().size() != 2) {
            return false;
        }
        Expression left = filter.getInputs().get(0);
        Expression right = filter.getInputs().get(1);
        if (left instanceof FieldExpression
            && table.isPartitionField(((FieldExpression) left).getFieldIndex())
            && right instanceof LiteralExpression) {
            return true;
        }
        return right instanceof FieldExpression
            && table.isPartitionField(((FieldExpression) right).getFieldIndex())
            && left instanceof LiteralExpression;
    }

    private static class PartitionFilterImpl implements PartitionFilter {

        private final Expression filter;

        public PartitionFilterImpl(Expression filter, List<Integer> partitionFields) {
            this.filter = filter.replace(exp -> {
                if (exp instanceof FieldExpression) {
                    FieldExpression field = (FieldExpression) exp;
                    int indexOfPartitionField = partitionFields.indexOf(field.getFieldIndex());
                    assert indexOfPartitionField >= 0 : "Not a partition field in partition filter";
                    return field.copy(indexOfPartitionField);
                }
                return exp;
            });
        }

        @Override
        public boolean apply(Row partition) {
            Boolean accept = (Boolean) filter.evaluate(partition);
            return accept != null && accept;
        }
    }
}
