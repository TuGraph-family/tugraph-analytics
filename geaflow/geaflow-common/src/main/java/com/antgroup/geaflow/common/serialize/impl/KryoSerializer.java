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

package com.antgroup.geaflow.common.serialize.impl;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.serialize.ISerializer;
import com.antgroup.geaflow.common.serialize.kryo.SubListSerializers4Jdk9;
import com.antgroup.geaflow.common.utils.ClassUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSortedSetSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSerializer implements ISerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KryoSerializer.class);
    private static final int INITIAL_BUFFER_SIZE = 4096;
    private static List<String> needRegisterClasses;
    private static Map<Class, Serializer> registeredSerializers;

    private final ThreadLocal<Kryo> local = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            Kryo.DefaultInstantiatorStrategy is = new Kryo.DefaultInstantiatorStrategy();
            is.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.setInstantiatorStrategy(is);

            kryo.getFieldSerializerConfig().setOptimizedGenerics(false);

            kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
            kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
            kryo.register(Collections.singletonList("").getClass(),
                new CollectionsSingletonListSerializer());
            kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

            ArrayListMultimapSerializer.registerSerializers(kryo);
            HashMultimapSerializer.registerSerializers(kryo);
            ImmutableListSerializer.registerSerializers(kryo);
            ImmutableMapSerializer.registerSerializers(kryo);
            ImmutableMultimapSerializer.registerSerializers(kryo);
            ImmutableSetSerializer.registerSerializers(kryo);
            ImmutableSortedSetSerializer.registerSerializers(kryo);
            LinkedHashMultimapSerializer.registerSerializers(kryo);
            LinkedListMultimapSerializer.registerSerializers(kryo);
            ReverseListSerializer.registerSerializers(kryo);
            TreeMultimapSerializer.registerSerializers(kryo);
            UnmodifiableNavigableSetSerializer.registerSerializers(kryo);
            SubListSerializers4Jdk9.addDefaultSerializers(kryo);
            UnmodifiableCollectionsSerializer.registerSerializers(kryo);

            ClassLoader tcl = Thread.currentThread().getContextClassLoader();
            if (tcl != null) {
                kryo.setClassLoader(tcl);
            }

            if (registeredSerializers != null) {
                for (Map.Entry<Class, Serializer> entry : registeredSerializers.entrySet()) {
                    LOGGER.info("register class:{} serializer", entry.getKey().getSimpleName());
                    kryo.register(entry.getKey(), entry.getValue());
                }
            }

            if (needRegisterClasses != null && needRegisterClasses.size() != 0) {
                for (String clazz : needRegisterClasses) {
                    String[] clazzToId = clazz.trim().split(":");
                    if (clazzToId.length != 2) {
                        throw new GeaflowRuntimeException("invalid clazzToId format:" + clazz);
                    }
                    int registerId = Integer.parseInt(clazzToId[1]);
                    registerClass(kryo, clazzToId[0], registerId);
                }
            }

            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.BinaryRow", 1011);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.DefaultParameterizedPath",
                "com.antgroup.geaflow.dsl.common.data.impl.DefaultParameterizedPath$DefaultParameterizedPathSerializer", 1012);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.DefaultParameterizedRow",
                "com.antgroup.geaflow.dsl.common.data.impl.DefaultParameterizedRow$DefaultParameterizedRowSerializer", 1013);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.DefaultPath",
                "com.antgroup.geaflow.dsl.common.data.impl.DefaultPath$DefaultPathSerializer", 1014);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId",
                "com.antgroup.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId$DefaultRowKeyWithRequestIdSerializer", 1015);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.ObjectRow",
                "com.antgroup.geaflow.dsl.common.data.impl.ObjectRow$ObjectRowSerializer", 1016);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.ObjectRowKey",
                "com.antgroup.geaflow.dsl.common.data.impl.ObjectRowKey$ObjectRowKeySerializer", 1017);

            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.BinaryStringEdge", 1018);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.BinaryStringTsEdge", 1019);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.BinaryStringVertex", 1020);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.DoubleEdge", 1021);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.DoubleTsEdge", 1022);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.DoubleVertex", 1023);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.IntEdge", 1024);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.IntTsEdge", 1025);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.IntVertex", 1026);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.LongEdge", 1027);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.LongTsEdge", 1028);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.LongVertex", 1029);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.ObjectEdge", 1030);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.ObjectTsEdge", 1031);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.types.ObjectVertex", 1032);

            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignEdge",
                "com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignEdge$FieldAlignEdgeSerializer", 1033);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignPath",
                "com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignPath$FieldAlignPathSerializer", 1034);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignVertex",
                "com.antgroup.geaflow.dsl.runtime.traversal.data.FieldAlignVertex$FieldAlignVertexSerializer", 1035);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex", 1036);

            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.ParameterizedRow", 1037);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.Path", 1038);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.Row", 1039);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.RowEdge", 1040);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.RowKey", 1041);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.RowKeyWithRequestId", 1042);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.RowVertex", 1043);
            registerClass(kryo, "com.antgroup.geaflow.dsl.common.data.impl.ParameterizedPath", 1044);

            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.EODMessage",
                "com.antgroup.geaflow.dsl.runtime.traversal.message.EODMessage$EODMessageSerializer", 1045);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.IPathMessage", 1046);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.JoinPathMessage",
                "com.antgroup.geaflow.dsl.runtime.traversal.message.JoinPathMessage$JoinPathMessageSerializer", 1047);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.KeyGroupMessage", 1048);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.KeyGroupMessageImpl",
                "com.antgroup.geaflow.dsl.runtime.traversal.message.KeyGroupMessageImpl$KeyGroupMessageImplSerializer", 1049);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage",
                "com.antgroup.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage$ParameterRequestMessageSerializer", 1050);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.RequestIsolationMessage", 1051);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessage", 1052);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl",
                "com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl$ReturnMessageImplSerializer", 1053);

            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.AbstractSingleTreePath", 1054);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.AbstractTreePath", 1055);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.EdgeTreePath",
                "com.antgroup.geaflow.dsl.runtime.traversal.path.EdgeTreePath$EdgeTreePathSerializer", 1056);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.EmptyTreePath", 1057);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath", 1058);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath",
                "com.antgroup.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath$ParameterizedTreePathSerializer", 1059);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.SourceEdgeTreePath",
                "com.antgroup.geaflow.dsl.runtime.traversal.path.SourceEdgeTreePath$SourceEdgeTreePathSerializer", 1060);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.SourceVertexTreePath",
                "com.antgroup.geaflow.dsl.runtime.traversal.path.SourceVertexTreePath$SourceVertexTreePathSerializer", 1061);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.UnionTreePath",
                "com.antgroup.geaflow.dsl.runtime.traversal.path.UnionTreePath$UnionTreePathSerializer", 1062);
            registerClass(kryo, "com.antgroup.geaflow.dsl.runtime.traversal.path.VertexTreePath",
                "com.antgroup.geaflow.dsl.runtime.traversal.path.VertexTreePath$VertexTreePathSerializer", 1063);

            return kryo;
        }
    };

    private void registerClass(Kryo kryo, String className, int kryoId) {
        try {
            LOGGER.debug("register class:{} id:{}", className, kryoId);
            Class<?> clazz = ClassUtil.classForName(className);
            kryo.register(clazz, kryoId);
        } catch (GeaflowRuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                LOGGER.warn("class not found: {} skip register id:{}", className, kryoId);
            }
        } catch (Throwable e) {
            LOGGER.error("error in register class: {} to kryo.", className);
            throw new GeaflowRuntimeException(e);
        }
    }

    private void registerClass(Kryo kryo, String className, String serializerClassName, int kryoId) {
        try {
            LOGGER.debug("register class:{} id:{}", className, kryoId);
            Class<?> clazz = ClassUtil.classForName(className);
            Class<?> serializerClazz = ClassUtil.classForName(serializerClassName);
            Serializer serializer = (Serializer) serializerClazz.newInstance();
            kryo.register(clazz, serializer, kryoId);
        } catch (GeaflowRuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                LOGGER.warn("class not found: {} skip register id:{}", className, kryoId);
            }
        } catch (Throwable e) {
            LOGGER.error("error in register class: {} to kryo.", className);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(Object o) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        Output output = new Output(outputStream);
        try {
            local.get().writeClassAndObject(output, o);
            output.flush();
        } finally {
            output.clear();
            output.close();
        }
        return outputStream.toByteArray();
    }

    @Override
    public Object deserialize(byte[] bytes) {
        Input input = new Input(bytes);
        return local.get().readClassAndObject(input);
    }

    @Override
    public void serialize(Object o, OutputStream outputStream) {
        Output output = new Output(outputStream);
        try {
            local.get().writeClassAndObject(output, o);
            output.flush();
        } finally {
            output.clear();
            output.close();
        }
    }

    @Override
    public Object deserialize(InputStream inputStream) {
        Input input = new Input(inputStream);
        return local.get().readClassAndObject(input);
    }

    public Kryo getThreadKryo() {
        return local.get();
    }

    @Override
    public <T> T copy(T target) {
        return local.get().copy(target);
    }

    public void clean() {
        local.remove();
    }
}
