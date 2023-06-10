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

package com.antgroup.geaflow.state.schema;


import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.schema.Field;
import com.antgroup.geaflow.common.serialize.ISerializer;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.meta.GraphFiledName;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.IGraphElementMeta;
import com.antgroup.geaflow.model.graph.property.EmptyProperty;
import com.antgroup.geaflow.model.graph.property.IPropertySerializable;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.graph.encoder.VertexAtom;
import com.google.common.base.Preconditions;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GraphDataSchema {

    private final IGraphElementMeta vertexMeta;
    private final IGraphElementMeta edgeMeta;
    private final Map<Class, Integer> metaIdMap = new HashMap<>();
    private final Map<Integer, Class> idMetaMap = new HashMap<>();
    private final boolean emptyVertexProperty;
    private final boolean emptyEdgeProperty;
    private Supplier<IVertex> vertexConsFun;
    private Supplier<IEdge> edgeConsFun;

    private Function<Object, byte[]> vertexPropertySerFun;
    private Function<Object, byte[]> edgePropertySerFun;
    private Function<byte[], Object> vertexPropertyDeFun;
    private Function<byte[], Object> edgePropertyDeFun;
    private List<EdgeAtom> edgeAtoms = new ArrayList<>();
    private List<VertexAtom> vertexAtoms = new ArrayList<>();
    private IType keyType;

    // Currently, only one schema is supported. Multiple schemas need to be considered when adding HLA.
    @SuppressWarnings("unchecked")
    public GraphDataSchema(GraphMeta meta) {
        this.vertexMeta = meta.getVertexMeta();
        this.edgeMeta = meta.getEdgeMeta();
        this.keyType = meta.getKeyType();
        this.emptyVertexProperty = this.vertexMeta.getPropertyClass() == EmptyProperty.class;
        this.emptyEdgeProperty = this.edgeMeta.getPropertyClass() == EmptyProperty.class;
        vertexConsFun = Objects.requireNonNull(
            (Supplier<IVertex>) meta.getVertexMeta().getGraphElementConstruct());

        edgeConsFun = Objects.requireNonNull(
            (Supplier<IEdge>) meta.getEdgeMeta().getGraphElementConstruct());

        transform(meta.getVertexMeta());
        transform(meta.getEdgeMeta());
    }

    private LinkedHashMap<String, IType> transform(IGraphElementMeta<?> elementMeta) {
        metaIdMap.put(elementMeta.getGraphElementClass(), (int) elementMeta.getGraphElementId());
        idMetaMap.put((int) elementMeta.getGraphElementId(), elementMeta.getGraphElementClass());

        boolean isEdge = IEdge.class.isAssignableFrom(elementMeta.getGraphElementClass());
        LinkedHashMap<String, IType> map = new LinkedHashMap<>();
        for (Field field : elementMeta.getGraphMeta().getFields()) {
            map.put(field.getName(), field.getType());
            if (isEdge) {
                edgeAtoms.add(Preconditions.checkNotNull(
                    EdgeAtom.EDGE_ATOM_MAP.get(GraphFiledName.valueOf(field.getName()))));
            } else {
                vertexAtoms.add(Preconditions.checkNotNull(
                    VertexAtom.VERTEX_ATOM_MAP.get(GraphFiledName.valueOf(field.getName()))));
            }
        }
        Tuple<Function<Object, byte[]>, Function<byte[], Object>> tuple =
            getPropertySerde(elementMeta.getPropertyClass());

        if (isEdge) {
            this.edgePropertySerFun = tuple.f0;
            this.edgePropertyDeFun = tuple.f1;
        } else {
            this.vertexPropertySerFun = tuple.f0;
            this.vertexPropertyDeFun = tuple.f1;
        }
        return map;
    }

    private Tuple<Function<Object, byte[]>, Function<byte[], Object>> getPropertySerde(Class<?> propertyClass) {
        Function<Object, byte[]> serFun;
        Function<byte[], Object> deFun;
        IType type = Types.getType(propertyClass);
        if (type != null) {
            serFun = type::serialize;
            deFun = type::deserialize;
            return Tuple.of(serFun, deFun);
        }

        boolean cloneable = IPropertySerializable.class.isAssignableFrom(propertyClass);
        if (cloneable && isFinalClass(propertyClass)) {
            serFun = o -> ((IPropertySerializable) o).toBytes();
            IPropertySerializable cleanProperty;
            try {
                cleanProperty = (IPropertySerializable) propertyClass.newInstance();
            } catch (Exception e) {
                throw new GeaflowRuntimeException(e);
            }
            deFun = bytes -> {
                IPropertySerializable clone = cleanProperty.clone();
                clone.fromBinary(bytes);
                return clone;
            };
        } else {
            ISerializer kryoSerializer = SerializerFactory.getKryoSerializer();
            serFun = kryoSerializer::serialize;
            deFun = kryoSerializer::deserialize;
        }
        return Tuple.of(serFun, deFun);
    }

    private boolean isFinalClass(Class clazz) {
        return Modifier.isFinal(clazz.getModifiers());
    }

    public Map<Class, Integer> getMetaIdMap() {
        return metaIdMap;
    }

    public Map<Integer, Class> getIdMetaMap() {
        return idMetaMap;
    }

    public IGraphElementMeta getVertexMeta() {
        return vertexMeta;
    }

    public IGraphElementMeta getEdgeMeta() {
        return edgeMeta;
    }

    public Supplier<IVertex> getVertexConsFun() {
        return vertexConsFun;
    }

    public Supplier<IEdge> getEdgeConsFun() {
        return edgeConsFun;
    }

    public Function<Object, byte[]> getVertexPropertySerFun() {
        return vertexPropertySerFun;
    }

    public Function<Object, byte[]> getEdgePropertySerFun() {
        return edgePropertySerFun;
    }

    public Function<byte[], Object> getVertexPropertyDeFun() {
        return vertexPropertyDeFun;
    }

    public Function<byte[], Object> getEdgePropertyDeFun() {
        return edgePropertyDeFun;
    }

    public IType getKeyType() {
        return keyType;
    }

    public List<EdgeAtom> getEdgeAtoms() {
        return edgeAtoms;
    }

    public void setEdgeAtoms(List<EdgeAtom> list) {
        Set<GraphFiledName> edgeFieldSet = this.edgeAtoms.stream().map(EdgeAtom::getGraphFieldName)
            .collect(Collectors.toSet());
        Set<GraphFiledName> newFieldSet = list.stream().map(EdgeAtom::getGraphFieldName)
            .collect(Collectors.toSet());
        Preconditions.checkArgument(edgeFieldSet.equals(newFieldSet),
            "edge element not match %s, elements are %s", list, edgeFieldSet);
        this.edgeAtoms = list;
    }

    public List<VertexAtom> getVertexAtoms() {
        return vertexAtoms;
    }

    public void setVertexAtoms(List<VertexAtom> list) {
        Set<GraphFiledName> vertexFieldSet =
            this.vertexAtoms.stream().map(VertexAtom::getGraphFieldName)
                .collect(Collectors.toSet());
        Set<GraphFiledName> newFieldSet = list.stream().map(VertexAtom::getGraphFieldName)
            .collect(Collectors.toSet());
        Preconditions.checkArgument(vertexFieldSet.equals(newFieldSet),
            "illegal vertex order " + list);
        this.vertexAtoms = list;
    }

    public boolean isEmptyVertexProperty() {
        return emptyVertexProperty;
    }

    public boolean isEmptyEdgeProperty() {
        return emptyEdgeProperty;
    }
}
