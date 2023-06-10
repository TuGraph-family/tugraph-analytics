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

package com.antgroup.geaflow.common.utils;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

public class ClassUtil {

    public static <O> Class<O> classForName(String className) {
        return classForName(className, true);
    }

    public static <O> Class<O> classForName(String className, boolean initialize) {
        try {
            return (Class<O>) Class.forName(className, initialize, getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public static <O> Class<O> classForName(String className, ClassLoader classLoader) {
        try {
            return (Class<O>) Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new GeaflowRuntimeException("fail to load class:" + className, e);
        }
    }

    public static ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = ClassUtil.class.getClassLoader();
        }
        return classLoader;
    }

    public static <O> O newInstance(Class<O> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GeaflowRuntimeException("fail to create instance for: " + clazz, e);
        }
    }

    public static <O> O newInstance(String className) {
        Class<O> clazz = classForName(className, Thread.currentThread().getContextClassLoader());
        return newInstance(clazz);
    }
}
