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

package com.antgroup.geaflow.console.biz.shared.aspect;

import com.antgroup.geaflow.console.core.model.GeaflowId;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ViewConverterAspect {

    @Around("execution(* com.antgroup.geaflow.console.biz.shared.convert.*Converter.convert(..))")
    public Object handle(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs();
        if (args[0] == null) {
            return null;
        }

        Object result = joinPoint.proceed(args);

        if (result instanceof GeaflowId) {
            GeaflowId geaflowId = (GeaflowId) result;
            geaflowId.validate();
        }

        return result;
    }
}
