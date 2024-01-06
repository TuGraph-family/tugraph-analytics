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

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.io.ResponseOutputDesc;
import com.antgroup.geaflow.shuffle.ForwardOutputDesc;
import com.antgroup.geaflow.shuffle.IOutputDesc;

public class CollectorFactory {

    public static <T> ICollector<T> create(IOutputDesc outputDesc) {
        switch (outputDesc.getType()) {
            case FORWARD:
                return new ForwardOutputCollector<>((ForwardOutputDesc) outputDesc);
            case LOOP:
                return new IterationOutputCollector<>((ForwardOutputDesc) outputDesc);
            case RESPONSE:
                return new CollectResponseCollector<>((ResponseOutputDesc) outputDesc);
            default:
                throw new GeaflowRuntimeException("not support output type {}" + outputDesc.getType());

        }
    }
}
