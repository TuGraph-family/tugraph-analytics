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

package com.antgroup.geaflow.console.common.util.proxy;

import com.antgroup.geaflow.console.common.util.LoaderSwitchUtil;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class GeaflowInvocationHandler implements InvocationHandler {

    private final ClassLoader targetClassLoader;

    private final Object targetInstance;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            Class<?> targetClass = targetInstance.getClass();

            return LoaderSwitchUtil.call(targetClassLoader, () -> {
                // prepare target args
                Object[] targetArgs = ProxyUtil.getTargetArgs(args);

                // prepare target method
                Method targetMethod = ProxyUtil.getTargetMethod(method, targetClass);

                // invoke target method
                Object targetResult = targetMethod.invoke(targetInstance, targetArgs);

                // proxy target result
                return ProxyUtil.proxyInstance(targetClassLoader, method.getGenericReturnType(), targetResult);
            });
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                throw ((InvocationTargetException) e).getTargetException();
            }
            throw e;
        }
    }
}
