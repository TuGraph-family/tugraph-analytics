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
import com.antgroup.geaflow.shuffle.message.Shard;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputDescriptor implements Serializable {

    // Map<edgeId, shard list> is upstream input shard result mapping info.
    private Map<Integer, List<Shard>> inputShardMap;
    // The mapping relation from input stream id to stream name.
    private Map<Integer, String> streamId2NameMap;
    private Map<Integer, IEncoder<?>> edgeId2EncoderMap;

    private ShuffleDescriptor shuffleDescriptor;

    public InputDescriptor() {
    }

    public Map<Integer, List<Shard>> getInputShardMap() {
        return inputShardMap;
    }

    public void setInputShardMap(Map<Integer, List<Shard>> inputShardMap) {
        this.inputShardMap = inputShardMap;
    }

    public Map<Integer, String> getStreamId2NameMap() {
        return streamId2NameMap;
    }

    public void setStreamId2NameMap(Map<Integer, String> streamId2NameMap) {
        this.streamId2NameMap = streamId2NameMap;
    }

    public Map<Integer, IEncoder<?>> getEdgeId2EncoderMap() {
        return this.edgeId2EncoderMap;
    }

    public void setEdgeId2EncoderMap(Map<Integer, IEncoder<?>> edgeId2EncoderMap) {
        this.edgeId2EncoderMap = edgeId2EncoderMap;
    }

    public ShuffleDescriptor getShuffleDescriptor() {
        return shuffleDescriptor;
    }

    public void setShuffleDescriptor(ShuffleDescriptor shuffleDescriptor) {
        this.shuffleDescriptor = shuffleDescriptor;
    }

    public InputDescriptor clone() {
        InputDescriptor inputDescriptor = new InputDescriptor();
        if (this.inputShardMap != null) {
            inputDescriptor.setInputShardMap(new HashMap<>(this.inputShardMap));
        }
        if (this.streamId2NameMap != null) {
            inputDescriptor.setStreamId2NameMap(new HashMap<>(this.streamId2NameMap));
        }
        if (this.shuffleDescriptor != null) {
            inputDescriptor.setShuffleDescriptor(shuffleDescriptor);
        }
        if (this.edgeId2EncoderMap != null) {
            inputDescriptor.setEdgeId2EncoderMap(this.edgeId2EncoderMap);
        }
        return inputDescriptor;
    }

}
