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

package com.antgroup.geaflow.partitioner.impl;

import com.antgroup.geaflow.api.partition.IPartition;
import com.antgroup.geaflow.api.partition.kv.KeyByPartition;

public class KeyPartitioner<T> extends AbstractPartitioner<T> {

    private int maxParallelism;

    public KeyPartitioner(int opId) {
        super(opId);
    }

    public void init(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    @Override
    public IPartition<T> getPartition() {
        return new KeyByPartition<T>(maxParallelism);
    }

    @Override
    public PartitionType getPartitionType() {
        return PartitionType.key;
    }

}
