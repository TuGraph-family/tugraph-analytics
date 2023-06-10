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

import com.antgroup.geaflow.cluster.response.CollectResult;
import com.antgroup.geaflow.collector.AbstractCollector;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.collector.IResultCollector;
import java.util.ArrayList;
import java.util.List;

public class CollectCollector<T> extends AbstractCollector
    implements IResultCollector<CollectResult>, ICollector<T> {

    /**
     * A fixed id for collect collector to identify the result.
     */
    public static final int COLLECT_RESULT_ID = -1;
    private final List<T> buffer;

    public CollectCollector(int id) {
        super(id);
        this.buffer = new ArrayList<>();
    }


    @Override
    public void partition(T value) {
        buffer.add(value);
        this.outputMeter.mark();
    }

    @Override
    public void finish() {
    }

    @Override
    public String getTag() {
        return null;
    }

    @Override
    public void broadcast(T value) {

    }

    @Override
    public <KEY> void partition(KEY key, T value) {
        partition(value);
    }

    @Override
    public CollectResult collectResult() {
        return new CollectResult(COLLECT_RESULT_ID, new ArrayList<>(buffer));
    }
}
