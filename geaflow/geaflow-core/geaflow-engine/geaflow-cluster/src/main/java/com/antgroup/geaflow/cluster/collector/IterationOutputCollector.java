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

import com.antgroup.geaflow.cluster.response.ShardResult;
import com.antgroup.geaflow.collector.IResultCollector;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.shuffle.ForwardOutputDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IterationOutputCollector<T>
    extends AbstractPipelineOutputCollector<T> implements IResultCollector<ShardResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IterationOutputCollector.class);

    public IterationOutputCollector(ForwardOutputDesc outputDesc) {
        super(outputDesc);
    }

    @Override
    public CollectType getType() {
        return CollectType.LOOP;
    }
}
