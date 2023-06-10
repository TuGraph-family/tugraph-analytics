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

package com.antgroup.geaflow.selector.impl;

import com.antgroup.geaflow.api.partition.IPartition;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.selector.ISelector;

public class ChannelSelector implements ISelector {

    private int numChannels;
    private IPartition partition;

    public ChannelSelector(int numChannels, IPartitioner partitioner) {
        this.numChannels = numChannels;
        this.partition = partitioner.getPartition();
    }

    @Override
    public <KEY> int[] selectChannels(KEY partitionKey) {
        return partition.partition(partitionKey, numChannels);
    }

}
