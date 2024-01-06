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

package com.antgroup.geaflow.runtime.shuffle;

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.runtime.io.IInputDesc;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.List;

public class ShardInputDesc implements IInputDesc<Shard> {

    private int edgeId;
    private String edgeName;
    private List<Shard> shards;

    private IEncoder<?> encoder;
    private ShuffleDescriptor shuffleDescriptor;

    public ShardInputDesc(int edgeId, String edgeName, List<Shard> shards,
                          IEncoder<?> encoder, ShuffleDescriptor shuffleDescriptor) {
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.shards = shards;
        this.encoder = encoder;
        this.shuffleDescriptor = shuffleDescriptor;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public String getName() {
        return edgeName;
    }

    @Override
    public List<Shard> getInput() {
        return shards;
    }

    @Override
    public InputType getInputType() {
        return InputType.META;
    }

    public IEncoder<?> getEncoder() {
        return encoder;
    }

    public ShuffleDescriptor getShuffleDescriptor() {
        return shuffleDescriptor;
    }
}
