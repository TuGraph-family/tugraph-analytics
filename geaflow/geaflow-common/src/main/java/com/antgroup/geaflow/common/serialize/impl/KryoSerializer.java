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

            return kryo;
        }
    };

    private void registerClass(Kryo kryo, String className, int kryoId) {
        try {
            LOGGER.info("register class:{} id:{}", className, kryoId);
            Class<?> clazz = ClassUtil.classForName(className);
            kryo.register(clazz, kryoId);
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
