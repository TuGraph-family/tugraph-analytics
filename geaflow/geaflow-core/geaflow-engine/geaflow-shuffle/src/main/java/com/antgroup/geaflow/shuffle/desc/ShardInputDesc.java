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

package com.antgroup.geaflow.shuffle.desc;

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.BatchPhase;
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.List;

public class ShardInputDesc implements IInputDesc<Shard> {

    private final int edgeId;
    private final String edgeName;
    private final List<Shard> shards;

    private final IEncoder<?> encoder;
    private final DataExchangeMode dataExchangeMode;
    private final BatchPhase batchPhase;

    public ShardInputDesc(int edgeId,
                          String edgeName,
                          List<Shard> shards,
                          IEncoder<?> encoder,
                          DataExchangeMode dataExchangeMode,
                          BatchPhase batchPhase) {
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.shards = shards;
        this.encoder = encoder;
        this.dataExchangeMode = dataExchangeMode;
        this.batchPhase = batchPhase;
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

    public DataExchangeMode getDataExchangeMode() {
        return this.dataExchangeMode;
    }

    public BatchPhase getBatchPhase() {
        return this.batchPhase;
    }

    public int getSliceNum() {
        return this.isPrefetchRead() ? 1 : this.shards.stream().mapToInt(s -> s.getSlices().size()).sum();
    }

    public boolean isPrefetchWrite() {
        return this.dataExchangeMode == DataExchangeMode.BATCH && this.batchPhase == BatchPhase.PREFETCH_WRITE;
    }

    public boolean isPrefetchRead() {
        return this.dataExchangeMode == DataExchangeMode.BATCH && this.batchPhase == BatchPhase.PREFETCH_READ;
    }

}
