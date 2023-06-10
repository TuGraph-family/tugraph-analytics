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

package com.antgroup.geaflow.console.common.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class ReflectUtil {

    public static <R> List<Field> getFields(Class<? extends R> clazz, Class<R> root, Predicate<Field> filter) {
        if (clazz == null) {
            return null;
        }

        if (root == null) {
            return doGetFields(clazz, Object.class, filter);
        }

        return doGetFields(clazz, root, filter);
    }

    private static List<Field> doGetFields(Class<?> clazz, Class<?> root, Predicate<Field> filter) {
        List<Field> fields = new ArrayList<>();

        if (!root.equals(clazz)) {
            fields.addAll(doGetFields(clazz.getSuperclass(), root, filter));
        }

        Arrays.stream(clazz.getDeclaredFields()).filter(filter).forEach(fields::add);
        return fields;
    }

}
