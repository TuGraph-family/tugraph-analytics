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

import com.antgroup.geaflow.api.collector.Collector;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.RichWindowFunction;
import com.antgroup.geaflow.api.function.base.AggregateFunction;
import com.antgroup.geaflow.api.function.base.FilterFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.MapFunction;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.utils.ArrayUtil;
import com.antgroup.geaflow.common.utils.ClassUtil;
import com.antgroup.geaflow.dsl.common.binary.EncoderFactory;
import com.antgroup.geaflow.dsl.common.binary.encoder.EdgeEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.IBinaryEncoder;
import com.antgroup.geaflow.dsl.common.binary.encoder.VertexEncoder;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowKey;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRowKey;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.function.FunctionContext;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.ObjectType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.connector.api.TableConnector;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.connector.api.TableWritableConnector;
import com.antgroup.geaflow.dsl.connector.api.function.GeaFlowTableSinkFunction;
import com.antgroup.geaflow.dsl.connector.api.util.ConnectorFactory;
import com.antgroup.geaflow.dsl.planner.GQLJavaTypeFactory;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.runtime.SinkDataView;
import com.antgroup.geaflow.dsl.runtime.function.table.AggFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.CorrelateFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.GroupByFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.GroupByFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.table.JoinTableFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.OrderByFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.ProjectFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.WhereFunction;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicRelNode.PhysicRelNodeName;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

public class GeaFlowRuntimeTable implements RuntimeTable {

    private final QueryContext queryContext;

    private final IPipelineTaskContext context;

    private final PWindowStream<Row> pStream;

    public GeaFlowRuntimeTable(QueryContext queryContext, IPipelineTaskContext context,
                               PWindowStream<Row> pStream) {
        this.queryContext = Objects.requireNonNull(queryContext);
        this.context = Objects.requireNonNull(context);
        this.pStream = Objects.requireNonNull(pStream);
    }

    public GeaFlowRuntimeTable copyWithSetOptions(PWindowStream<Row> pStream) {
        pStream = pStream.withConfig(queryContext.getSetOptions());
        return new GeaFlowRuntimeTable(queryContext, context, pStream);
    }

    @Override
    public <T> T getPlan() {
        return (T) pStream;
    }

    @Override
    public List<Row> take() {
        throw new GeaFlowDSLException("take() has not support currently.");
    }

    @Override
    public RuntimeTable project(ProjectFunction function) {
        String opName = PhysicRelNodeName.PROJECT.getName(queryContext.getOpNameCount());
        int parallelism = queryContext.getConfigParallelisms(opName, pStream.getParallelism());

        PWindowStream<Row> map = pStream.map(new TableProjectFunction(function))
            .withName(opName).withParallelism(parallelism);
        return copyWithSetOptions(map);
    }

    @Override
    public RuntimeTable filter(WhereFunction function) {
        String opName = PhysicRelNodeName.FILTER.getName(queryContext.getOpNameCount());
        int parallelism = queryContext.getConfigParallelisms(opName, pStream.getParallelism());
        PWindowStream<Row> filter = pStream.filter(new TableFilterFunction(function))
            .withName(opName).withParallelism(parallelism);
        return copyWithSetOptions(filter);
    }

    @Override
    public RuntimeTable join(RuntimeTable other, JoinTableFunction function) {
        throw new GeaFlowDSLException("Join has not support yet");
    }

    @Override
    public RuntimeTable aggregate(GroupByFunction groupByFunction, AggFunction aggFunction) {
        String opName = PhysicRelNodeName.AGGREGATE.getName(queryContext.getOpNameCount());
        int parallelism = queryContext.getConfigParallelisms(opName, pStream.getParallelism());
        boolean isGlobalDistinct = aggFunction.getValueTypes().length == 0;
        PWindowStream<Row> aggregate =
            pStream.flatMap(isGlobalDistinct ? new TableLocalDistinctFunction(groupByFunction)
                                             : new TableLocalAggregateFunction(groupByFunction, aggFunction))
                .withName(opName + "-local")
                .withParallelism(pStream.getParallelism())
                .keyBy(new GroupKeySelectorFunction(groupByFunction))
                .withName(opName + "-KeyBy")
                .withParallelism(pStream.getParallelism())
                .materialize()
                .aggregate(isGlobalDistinct ? new TableGlobalDistinctFunction(groupByFunction)
                                            : new TableGlobalAggregateFunction(groupByFunction, aggFunction))
                .withName(opName + "-global")
                .withParallelism(parallelism);
        return copyWithSetOptions(aggregate);
    }

    @Override
    public RuntimeTable union(RuntimeTable other) {
        String opName = PhysicRelNodeName.UNION.getName(queryContext.getOpNameCount());
        int parallelism = queryContext.getConfigParallelisms(opName, pStream.getParallelism());
        PWindowStream<Row> union = pStream.union(other.getPlan())
            .withName(opName).withParallelism(parallelism);
        return copyWithSetOptions(union);
    }

    @Override
    public RuntimeTable orderBy(OrderByFunction function) {
        String opName = PhysicRelNodeName.SORT.getName(queryContext.getOpNameCount());
        PWindowStream<Row> order = pStream.flatMap(new TableOrderByFunction(function))
            .withName(opName + "-local").withParallelism(pStream.getParallelism())
            .flatMap(new TableOrderByFunction(function))
            .withName(opName + "-global")
            .withParallelism(1);
        return copyWithSetOptions(order);
    }

    @Override
    public RuntimeTable correlate(CorrelateFunction function) {
        String opName = PhysicRelNodeName.CORRELATE.getName(queryContext.getOpNameCount());
        int parallelism = queryContext.getConfigParallelisms(opName, pStream.getParallelism());
        PWindowStream<Row> correlate = pStream.flatMap(new CorrelateFlatMapFunction(function))
            .withName(opName).withParallelism(parallelism);
        return copyWithSetOptions(correlate);
    }

    @Override
    public SinkDataView write(GeaFlowTable table) {
        TableConnector connector = ConnectorFactory.loadConnector(table.getTableType());
        if (!(connector instanceof TableWritableConnector)) {
            throw new GeaFlowDSLException("Table: '{}' is not writeable", connector.getType());
        }
        TableWritableConnector writableConnector = (TableWritableConnector) connector;
        Configuration conf = table.getConfigWithGlobal(context.getConfig(), queryContext.getSetOptions());
        TableSink tableSink = writableConnector.createSink(conf);
        tableSink.init(conf, table.getTableSchema());

        String opName = PhysicRelNodeName.TABLE_SINK.getName(queryContext.getOpNameCount());
        int parallelism = queryContext.getConfigParallelisms(opName, pStream.getParallelism());

        GeaFlowTableSinkFunction sinkFunction;
        if (conf.contains(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SINK_FUNCTION)) {
            String customClassName = conf.getString(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SINK_FUNCTION);
            try {
                sinkFunction = (GeaFlowTableSinkFunction) ClassUtil.classForName(customClassName)
                    .getConstructor(GeaFlowTable.class, TableSink.class)
                    .newInstance(table, tableSink);
            } catch (Exception e) {
                throw new GeaFlowDSLException("Cannot create sink function: {}.",
                    customClassName, e);
            }
        } else {
            sinkFunction = new GeaFlowTableSinkFunction(table, tableSink);
        }
        PWindowStream<Row> inputStream = pStream;
        if (table.getPrimaryFields().size() > 0) {
            int[] primaryKeyIndices = ArrayUtil.toIntArray(table.getPrimaryFields()
                .stream().map(name -> table.getTableSchema().indexOf(name))
                .collect(Collectors.toList()));
            IType<?>[] primaryKeyTypes = table.getPrimaryFields()
                .stream().map(name -> table.getTableSchema().getField(name).getType())
                .collect(Collectors.toList()).toArray(new IType[]{});

            inputStream = pStream.keyBy(new GroupKeySelectorFunction(
                new GroupByFunctionImpl(primaryKeyIndices, primaryKeyTypes)));
        }
        PStreamSink<Row> sink = inputStream.sink(sinkFunction)
            .withConfig(queryContext.getSetOptions())
            .withName(opName)
            .withParallelism(parallelism);
        return new GeaFlowSinkDataView(context, sink);
    }

    @Override
    public SinkDataView write(GeaFlowGraph graph, QueryContext queryContext) {
        PWindowStream<RowVertex> vertexStream = pStream.flatMap(new RowToVertexFunction(graph));
        PWindowStream<RowEdge> edgeStream = pStream.flatMap(new RowToEdgeFunction(graph));

        PWindowStream<RowVertex> preVertexStream = queryContext.getGraphVertexStream(graph.getName());
        if (preVertexStream != null) {
            vertexStream = vertexStream.union(preVertexStream);
        }
        PWindowStream<RowEdge> preEdgeStream = queryContext.getGraphEdgeStream(graph.getName());
        if (preEdgeStream != null) {
            edgeStream = edgeStream.union(preEdgeStream);
        }
        queryContext.updateVertexAndEdgeToGraph(graph.getName(), graph, vertexStream, edgeStream);
        return new GeaFlowSinkIncGraphView(context);
    }

    private static class TableProjectFunction extends RichFunction implements MapFunction<Row, Row>, Serializable {

        private boolean skipException;

        private final ProjectFunction projectFunction;

        public TableProjectFunction(ProjectFunction projectFunction) {
            this.projectFunction = projectFunction;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            this.skipException = runtimeContext.getConfiguration()
                .getBoolean(DSLConfigKeys.GEAFLOW_DSL_SKIP_EXCEPTION);
        }

        @Override
        public Row map(Row value) {
            try {
                return projectFunction.project(value);
            } catch (Exception e) {
                if (!skipException) {
                    throw new GeaFlowDSLException(e);
                }
                return null;
            }
        }

        @Override
        public void close() {

        }
    }

    private static class TableFilterFunction extends RichFunction implements FilterFunction<Row> {

        private boolean skipException;

        private final WhereFunction whereFunction;

        public TableFilterFunction(WhereFunction whereFunction) {
            this.whereFunction = whereFunction;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            this.skipException = runtimeContext.getConfiguration()
                .getBoolean(DSLConfigKeys.GEAFLOW_DSL_SKIP_EXCEPTION);
        }

        @Override
        public boolean filter(Row record) {
            try {
                return whereFunction.filter(record);
            } catch (Exception e) {
                if (!skipException) {
                    throw new GeaFlowDSLException(e);
                }
                return false;
            }
        }

        @Override
        public void close() {

        }
    }

    private static class TableLocalDistinctFunction extends RichWindowFunction implements
        FlatMapFunction<Row, Row> {

        private final GroupByFunction groupByFunction;
        private final Map<RowKey, Row> aggregatingState;
        private final IBinaryEncoder encoder;

        public TableLocalDistinctFunction(GroupByFunction groupByFunction) {
            this.groupByFunction = groupByFunction;
            this.aggregatingState = new HashMap<>();
            IType<?>[] fieldTypes = groupByFunction.getFieldTypes();
            TableField[] tableFields = new TableField[fieldTypes.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                tableFields[i] = new TableField(String.valueOf(i), fieldTypes[i], false);
            }
            this.encoder = EncoderFactory.createEncoder(new StructType(tableFields));
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public void flatMap(Row value, Collector<Row> collector) {
            //local distinct
            RowKey groupKey = groupByFunction.getRowKey(value);
            Row acc = aggregatingState.get(groupKey);
            if (acc == null) {
                assert collector != null : "collector is null";
                IType<?>[] keyTypes = groupByFunction.getFieldTypes();
                Object[] fields = new Object[keyTypes.length];
                for (int i = 0; i < keyTypes.length; i++) {
                    fields[i] = groupKey.getField(i, keyTypes[i]);
                }
                collector.partition(encoder.encode(ObjectRow.create(fields)));
            }
            aggregatingState.put(groupKey, value);
        }

        @Override
        public void finish() {
            aggregatingState.clear();
        }
    }

    private static class TableLocalAggregateFunction extends RichWindowFunction implements
        FlatMapFunction<Row, Row> {

        private final AggFunction localAggFunction;
        private final GroupByFunction groupByFunction;
        private Collector<Row> collector;
        private final Map<RowKey, Object> aggregatingState;
        private final IBinaryEncoder encoder;

        public TableLocalAggregateFunction(GroupByFunction groupByFunction, AggFunction localAggFunction) {
            this.localAggFunction = localAggFunction;
            this.groupByFunction = groupByFunction;
            this.aggregatingState = new HashMap<>();
            IType<?>[] fieldTypes = groupByFunction.getFieldTypes();
            TableField[] tableFields = new TableField[fieldTypes.length + 1];
            for (int i = 0; i < fieldTypes.length; i++) {
                tableFields[i] = new TableField(String.valueOf(i), fieldTypes[i], false);
            }
            tableFields[fieldTypes.length] = new TableField(String.valueOf(fieldTypes.length)
                , ObjectType.INSTANCE, false);
            this.encoder = EncoderFactory.createEncoder(new StructType(tableFields));
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            FunctionContext context =
                FunctionContext.of(runtimeContext.getConfiguration());
            localAggFunction.open(context);
        }

        @Override
        public void close() {
        }

        @Override
        public void flatMap(Row value, Collector<Row> collector) {
            this.collector = collector;
            //local aggregate
            RowKey groupKey = groupByFunction.getRowKey(value);
            Object acc = aggregatingState.get(groupKey);
            if (acc == null) {
                acc = localAggFunction.createAccumulator();
            }
            localAggFunction.add(value, acc);
            aggregatingState.put(groupKey, acc);
        }

        @Override
        public void finish() {
            for (Entry<RowKey, Object> rowKeyObjectEntry : aggregatingState.entrySet()) {
                assert collector != null : "collector is null";
                IType<?>[] keyTypes = groupByFunction.getFieldTypes();
                //The last offset of ObjectRow is accumulator
                Object[] fields = new Object[keyTypes.length + 1];
                for (int i = 0; i < keyTypes.length; i++) {
                    fields[i] = rowKeyObjectEntry.getKey().getField(i, keyTypes[i]);
                }
                fields[keyTypes.length] = rowKeyObjectEntry.getValue();
                collector.partition(encoder.encode(ObjectRow.create(fields)));
            }
            aggregatingState.clear();
        }
    }

    private static class TableGlobalDistinctFunction extends RichFunction implements
        AggregateFunction<Row, Object, Row> {

        private final GroupByFunction groupByFunction;

        public TableGlobalDistinctFunction(GroupByFunction groupByFunction) {
            this.groupByFunction = groupByFunction;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public Object createAccumulator() {
            return new DistinctAccumulator(null);
        }

        @Override
        public void add(Row value, Object keyAccumulator) {
            IType<?>[] keyTypes = groupByFunction.getFieldTypes();
            Object[] fields = new Object[keyTypes.length];
            for (int i = 0; i < keyTypes.length; i++) {
                fields[i] = value.getField(i, keyTypes[i]);
            }
            RowKey key = ObjectRowKey.of(fields);
            DistinctAccumulator keyAcc = (DistinctAccumulator) keyAccumulator;
            if (keyAcc.getKey() == null) {
                keyAcc.setKey(key);
            }
        }

        @Override
        public Row getResult(Object keyAccumulator) {
            RowKey key = ((DistinctAccumulator) keyAccumulator).getResult();
            if (key == null) {
                return null;
            }
            IType<?>[] keyTypes = groupByFunction.getFieldTypes();
            Object[] fields = new Object[keyTypes.length];
            for (int i = 0; i < keyTypes.length; i++) {
                fields[i] = key.getField(i, keyTypes[i]);
            }
            return ObjectRow.create(fields);
        }

        @Override
        public Object merge(Object a, Object b) {
            assert Objects.equals(((DistinctAccumulator) a).getKey(),
                ((DistinctAccumulator) b).getKey());
            return a;
        }

        private static class DistinctAccumulator implements Serializable {

            private RowKey key;

            private boolean hasBeenRead = false;

            public DistinctAccumulator(RowKey key) {
                this.key = key;
            }

            public RowKey getKey() {
                return key;
            }

            public void setKey(RowKey key) {
                this.key = key;
            }

            public RowKey getResult() {
                if (hasBeenRead) {
                    return null;
                } else {
                    hasBeenRead = true;
                    return key;
                }
            }
        }
    }

    private static class TableGlobalAggregateFunction extends RichFunction implements
        AggregateFunction<Row, Object, Row> {

        private final AggFunction aggFunction;
        private final GroupByFunction groupByFunction;

        public TableGlobalAggregateFunction(GroupByFunction groupByFunction, AggFunction aggFunction) {
            this.aggFunction = aggFunction;
            this.groupByFunction = groupByFunction;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            FunctionContext context =
                FunctionContext.of(runtimeContext.getConfiguration());
            aggFunction.open(context);
        }

        @Override
        public void close() {
        }

        @Override
        public Object createAccumulator() {
            return new KeyAccumulator(null, aggFunction.createAccumulator());
        }

        @Override
        public void add(Row value, Object keyAccumulator) {
            IType<?>[] keyTypes = groupByFunction.getFieldTypes();
            Object[] fields = new Object[keyTypes.length];
            for (int i = 0; i < keyTypes.length; i++) {
                fields[i] = value.getField(i, keyTypes[i]);
            }
            RowKey key = ObjectRowKey.of(fields);
            KeyAccumulator keyAcc = (KeyAccumulator) keyAccumulator;
            if (keyAcc.getKey() == null) {
                keyAcc.setKey(key);
            }
            if (aggFunction.getValueTypes().length > 0) {
                int offset = keyTypes.length;
                aggFunction.merge(keyAcc.getAcc(), value.getField(offset, ObjectType.INSTANCE));
            }
        }

        @Override
        public Row getResult(Object keyAccumulator) {
            KeyAccumulator keyAcc = (KeyAccumulator) keyAccumulator;
            RowKey key = keyAcc.getKey();
            Object accumulator = keyAcc.getAcc();
            Row aggValue = aggFunction.getValue(accumulator);

            IType<?>[] keyTypes = groupByFunction.getFieldTypes();
            IType<?>[] valueTypes = aggFunction.getValueTypes();

            Object[] fields = new Object[keyTypes.length + valueTypes.length];
            for (int i = 0; i < keyTypes.length; i++) {
                fields[i] = key.getField(i, keyTypes[i]);
            }

            int offset = keyTypes.length;
            for (int i = 0; i < valueTypes.length; i++) {
                fields[offset + i] = aggValue.getField(i, valueTypes[i]);
            }
            return ObjectRow.create(fields);
        }

        @Override
        public Object merge(Object a, Object b) {
            aggFunction.merge(((KeyAccumulator) a).getAcc(), ((KeyAccumulator) b).getAcc());
            return a;
        }

        private static class KeyAccumulator implements Serializable {

            private RowKey key;

            private final Object accumulator;

            public KeyAccumulator(RowKey key, Object accumulator) {
                this.key = key;
                this.accumulator = accumulator;
            }

            public RowKey getKey() {
                return key;
            }

            public void setKey(RowKey key) {
                this.key = key;
            }

            public Object getAcc() {
                return accumulator;
            }
        }
    }

    private static class GroupKeySelectorFunction implements KeySelector<Row, RowKey> {

        GroupByFunction groupByFunction;

        public GroupKeySelectorFunction(GroupByFunction groupByFunction) {
            this.groupByFunction = groupByFunction;
        }

        @Override
        public RowKey getKey(Row value) {
            IType<?>[] keyTypes = groupByFunction.getFieldTypes();
            Object[] fields = new Object[keyTypes.length];
            for (int i = 0; i < keyTypes.length; i++) {
                fields[i] = value.getField(i, keyTypes[i]);
            }
            return ObjectRowKey.of(fields);
        }
    }

    private static class TableOrderByFunction extends RichWindowFunction implements
        FlatMapFunction<Row, Row> {

        private final OrderByFunction orderByFunction;
        private Collector<Row> collector;

        public TableOrderByFunction(OrderByFunction orderByFunction) {
            this.orderByFunction = orderByFunction;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            FunctionContext context =
                FunctionContext.of(runtimeContext.getConfiguration());
            orderByFunction.open(context);
        }

        @Override
        public void flatMap(Row value, Collector<Row> collector) {
            this.orderByFunction.process(value);
            this.collector = collector;
        }

        @Override
        public void finish() {
            Iterable<Row> resultRows = orderByFunction.finish();
            for (Row row : resultRows) {
                assert collector != null : "Not empty sort encounters collector which is null";
                collector.partition(row);
            }
        }

        @Override
        public void close() {
        }
    }


    private static class CorrelateFlatMapFunction extends RichFunction implements FlatMapFunction<Row, Row> {

        private final CorrelateFunction correlateFunction;

        public CorrelateFlatMapFunction(CorrelateFunction correlateFunction) {
            this.correlateFunction = correlateFunction;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            FunctionContext context =
                FunctionContext.of(runtimeContext.getConfiguration());
            correlateFunction.open(context);
        }

        @Override
        public void close() {
        }

        @Override
        public void flatMap(Row value, Collector<Row> collector) {
            if (value != null) {
                List<Row> table = correlateFunction.process(value);
                if (table != null) {
                    List<Row> rows = joinTable(value, table);
                    for (Row row : rows) {
                        if (row != null) {
                            collector.partition(row);
                        }
                    }
                }
            }
        }

        private List<Row> joinTable(Row value, List<Row> table) {
            List<Row> rows = Lists.newArrayList();
            for (Row line : table) {
                Object[] values = new Object[correlateFunction.getLeftOutputTypes().size()
                    + correlateFunction.getRightOutputTypes().size()];
                int idx = 0;
                for (int i = 0; i < correlateFunction.getLeftOutputTypes().size(); i++) {
                    values[idx++] = value.getField(i, correlateFunction.getLeftOutputTypes().get(i));
                }
                for (int i = 0; i < correlateFunction.getRightOutputTypes().size(); i++) {
                    values[idx++] = line.getField(i, correlateFunction.getRightOutputTypes().get(i));
                }
                Row row = ObjectRow.create(values);
                rows.add(row);
            }
            return rows;
        }
    }

    /**
     * Convert {@link Row} to {@link RowVertex} for writing graph.
     */
    public static class RowToVertexFunction implements FlatMapFunction<Row, RowVertex> {

        private final int numVertex;

        private final IType<?>[] vertexTypes;

        private final VertexEncoder[] vertexEncoders;

        public RowToVertexFunction(GeaFlowGraph graph) {
            this.numVertex = graph.getVertexTables().size();
            this.vertexTypes = new IType[numVertex];
            this.vertexEncoders = new VertexEncoder[numVertex];

            GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();
            for (int i = 0; i < numVertex; i++) {
                vertexTypes[i] = SqlTypeUtil.convertType(
                    graph.getVertexTables().get(i).getRowType(typeFactory));
                vertexEncoders[i] = EncoderFactory.createVertexEncoder((VertexType) vertexTypes[i]);
            }
        }

        @Override
        public void flatMap(Row value, Collector<RowVertex> collector) {
            for (int i = 0; i < numVertex; i++) {
                RowVertex vertex = (RowVertex) value.getField(i, vertexTypes[i]);
                if (vertex != null) {
                    collector.partition(vertexEncoders[i].encode(vertex));
                }
            }
        }
    }

    /**
     * Convert {@link Row} to {@link RowEdge} for writing graph.
     */
    public static class RowToEdgeFunction implements FlatMapFunction<Row, RowEdge> {

        private final int numVertex;

        private final int numEdge;

        private final IType<?>[] edgeTypes;

        private final EdgeEncoder[] edgeEncoders;

        public RowToEdgeFunction(GeaFlowGraph graph) {
            this.numVertex = graph.getVertexTables().size();
            this.numEdge = graph.getEdgeTables().size();
            this.edgeTypes = new IType[numEdge];
            this.edgeEncoders = new EdgeEncoder[numEdge];
            GQLJavaTypeFactory typeFactory = GQLJavaTypeFactory.create();
            for (int i = 0; i < numEdge; i++) {
                edgeTypes[i] = SqlTypeUtil.convertType(
                    graph.getEdgeTables().get(i).getRowType(typeFactory));
                edgeEncoders[i] = EncoderFactory.createEdgeEncoder((EdgeType) edgeTypes[i]);
            }
        }

        @Override
        public void flatMap(Row value, Collector<RowEdge> collector) {
            for (int i = numVertex; i < numVertex + numEdge; i++) {
                RowEdge edge = (RowEdge) value.getField(i, edgeTypes[i - numVertex]);
                if (edge != null) {
                    RowEdge encodeEdge = edgeEncoders[i - numVertex].encode(edge);
                    collector.partition(encodeEdge);
                    collector.partition(encodeEdge.identityReverse());
                }
            }
        }
    }
}
