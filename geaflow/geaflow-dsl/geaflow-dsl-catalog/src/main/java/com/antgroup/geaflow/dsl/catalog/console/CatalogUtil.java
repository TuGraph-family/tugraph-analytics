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

package com.antgroup.geaflow.dsl.catalog.console;

import static com.antgroup.geaflow.dsl.util.SqlTypeUtil.convertTypeName;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CatalogUtil {

    public static TableModel convertToTableModel(GeaFlowTable table) {
        TableModel tableModel = new TableModel();
        PluginConfigModel pluginConfigModel = new PluginConfigModel();
        pluginConfigModel.setType(GeaFlowPluginType.getPluginType(table.getTableType()));
        pluginConfigModel.setConfig(table.getConfig());
        tableModel.setPluginConfig(pluginConfigModel);
        tableModel.setName(table.getName());
        List<TableField> fields = table.getFields();
        List<FieldModel> fieldModelList = convertToFieldModel(fields);
        tableModel.setFields(fieldModelList);
        return tableModel;
    }

    public static GeaFlowTable convertToGeaFlowTable(TableModel model, String instanceName) {
        if (model == null) {
            return null;
        }
        List<FieldModel> fieldModels = model.getFields();
        List<TableField> fields = convertToTableField(fieldModels);
        return new GeaFlowTable(instanceName, model.getName(), fields, new ArrayList<>(),
            new ArrayList<>(), model.getPluginConfig().getConfig(), true, false);
    }

    public static VertexModel convertToVertexModel(VertexTable table) {
        VertexModel vertexModel = new VertexModel();
        vertexModel.setName(table.getTypeName());
        List<TableField> fields = table.getFields();
        String idFieldName = table.getIdFieldName();
        List<FieldModel> fieldModels = new ArrayList<>(fields.size());
        for (TableField field : fields) {
            GeaFlowFieldCategory fieldCategory;
            if (field.getName().equals(idFieldName)) {
                fieldCategory = GeaFlowFieldCategory.VERTEX_ID;
            } else {
                fieldCategory = GeaFlowFieldCategory.PROPERTY;
            }
            fieldModels.add(new FieldModel(field.getName(), null,
                GeaFlowFieldType.getFieldType(field.getType()), fieldCategory));
        }
        vertexModel.setFields(fieldModels);
        return vertexModel;
    }

    public static VertexTable convertToVertexTable(VertexModel model) {
        List<FieldModel> fieldModels = model.getFields();
        List<TableField> fields = new ArrayList<>(fieldModels.size());
        String idFieldName = null;
        for (FieldModel fieldModel : fieldModels) {
            if (fieldModel.getCategory() == GeaFlowFieldCategory.VERTEX_ID) {
                idFieldName = fieldModel.getName();
            }
            String typeName = convertTypeName(fieldModel.getType().name());
            IType<?> fieldType = Types.of(typeName);
            TableField field = new TableField(fieldModel.getName(), fieldType, false);
            fields.add(field);
        }
        return new VertexTable(model.getName(), fields, idFieldName);
    }

    public static EdgeModel convertToEdgeModel(EdgeTable table) {
        EdgeModel edgeModel = new EdgeModel();
        edgeModel.setName(table.getTypeName());
        List<TableField> fields = table.getFields();
        List<FieldModel> fieldModels = new ArrayList<>(fields.size());
        String srcIdFieldName = table.getSrcIdFieldName();
        String targetIdFieldName = table.getTargetIdFieldName();
        String timestampFieldName = table.getTimestampFieldName();
        for (TableField field : fields) {
            GeaFlowFieldCategory fieldCategory;
            if (field.getName().equals(srcIdFieldName)) {
                fieldCategory = GeaFlowFieldCategory.EDGE_SOURCE_ID;
            } else if (field.getName().equals(targetIdFieldName)) {
                fieldCategory = GeaFlowFieldCategory.EDGE_TARGET_ID;
            } else if (field.getName().equals(timestampFieldName)) {
                fieldCategory = GeaFlowFieldCategory.EDGE_TIMESTAMP;
            } else {
                fieldCategory = GeaFlowFieldCategory.PROPERTY;
            }
            fieldModels.add(new FieldModel(field.getName(), null,
                GeaFlowFieldType.getFieldType(field.getType()), fieldCategory));
        }
        edgeModel.setFields(fieldModels);
        return edgeModel;
    }

    public static EdgeTable convertToEdgeTable(EdgeModel model) {
        List<FieldModel> fieldModels = model.getFields();
        List<TableField> fields = new ArrayList<>(fieldModels.size());
        String srcIdFieldName = null;
        String targetIdFieldName = null;
        String timestampFieldName = null;
        for (FieldModel fieldModel : fieldModels) {
            switch (fieldModel.getCategory()) {
                case EDGE_SOURCE_ID:
                    srcIdFieldName = fieldModel.getName();
                    break;
                case EDGE_TARGET_ID:
                    targetIdFieldName = fieldModel.getName();
                    break;
                case EDGE_TIMESTAMP:
                    timestampFieldName = fieldModel.getName();
                    break;
                default:
            }
            String typeName = convertTypeName(fieldModel.getType().name());
            IType<?> fieldType = Types.of(typeName);
            TableField field = new TableField(fieldModel.getName(), fieldType, false);
            fields.add(field);
        }
        return new EdgeTable(model.getName(), fields, srcIdFieldName, targetIdFieldName,
            timestampFieldName);
    }

    public static GraphModel convertToGraphModel(GeaFlowGraph graph) {
        GraphModel graphModel = new GraphModel();
        graphModel.setStaticGraph(graph.isStatic());
        PluginConfigModel pluginConfigModel = new PluginConfigModel();
        pluginConfigModel.setType(GeaFlowPluginType.getPluginType(graph.getStoreType()));
        pluginConfigModel.setConfig(graph.getConfig().getConfigMap());

        graphModel.setPluginConfig(pluginConfigModel);
        List<VertexTable> vertexTables = graph.getVertexTables();
        List<VertexModel> vertexModels = new ArrayList<>(vertexTables.size());
        for (VertexTable vertexTable : vertexTables) {
            vertexModels.add(convertToVertexModel(vertexTable));
        }
        graphModel.setVertices(vertexModels);
        List<EdgeTable> edgeTables = graph.getEdgeTables();
        List<EdgeModel> edgeModels = new ArrayList<>(edgeTables.size());
        for (EdgeTable edgeTable : edgeTables) {
            edgeModels.add(convertToEdgeModel(edgeTable));
        }
        graphModel.setEdges(edgeModels);
        graphModel.setName(graph.getName());
        return graphModel;
    }

    public static GeaFlowGraph convertToGeaFlowGraph(GraphModel model, String instanceName) {
        if (model == null) {
            return null;
        }
        List<VertexModel> vertices = model.getVertices();
        List<VertexTable> vertexTables = new ArrayList<>(vertices.size());
        for (VertexModel vertexModel : vertices) {
            vertexTables.add(convertToVertexTable(vertexModel));
        }
        List<EdgeModel> edges = model.getEdges();
        List<EdgeTable> edgeTables = new ArrayList<>(edges.size());
        for (EdgeModel edgeModel : edges) {
            edgeTables.add(convertToEdgeTable(edgeModel));
        }
        boolean isStaticGraph = model.isStaticGraph();
        return new GeaFlowGraph(instanceName, model.getName(), vertexTables, edgeTables,
            model.getPluginConfig().getConfig(), Collections.emptyMap(), true, isStaticGraph, false);
    }

    private static List<FieldModel> convertToFieldModel(List<TableField> fields) {
        List<FieldModel> fieldModelList = new ArrayList<>(fields.size());
        for (TableField field : fields) {
            FieldModel fieldModel = new FieldModel(field.getName(), null,
                GeaFlowFieldType.getFieldType(field.getType()), GeaFlowFieldCategory.PROPERTY);
            fieldModelList.add(fieldModel);
        }
        return fieldModelList;
    }

    private static List<TableField> convertToTableField(List<FieldModel> fieldModels) {
        List<TableField> fields = new ArrayList<>(fieldModels.size());
        for (FieldModel fieldModel : fieldModels) {
            String typeName = convertTypeName(fieldModel.getType().name());
            IType<?> fieldType = Types.of(typeName);
            TableField field = new TableField(fieldModel.getName(), fieldType, false);
            fields.add(field);
        }
        return fields;
    }
}
