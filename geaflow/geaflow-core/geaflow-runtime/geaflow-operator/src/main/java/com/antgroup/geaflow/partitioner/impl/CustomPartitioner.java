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

public class CustomPartitioner<T> extends AbstractPartitioner<T> {

    private final IPartition partition;

    public CustomPartitioner(int opId, IPartition<T> partition) {
        super(opId);
        this.partition = partition;
    }

    @Override
    public IPartition<T> getPartition() {
        return this.partition;
    }

    @Override
    public PartitionType getPartitionType() {
        return PartitionType.custom;
    }
}
