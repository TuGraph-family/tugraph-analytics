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

package com.antgroup.geaflow.shuffle.message;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;

public class RemoteShuffleMeta extends BaseSliceMeta {

    private final ShuffleId shuffleId;
    private ShuffleAddress address;

    public RemoteShuffleMeta(int sourceIndex, int targetIndex, ShuffleId shuffleId,
                             long encodedSize) {
        super(sourceIndex, targetIndex);
        this.shuffleId = shuffleId;
        setEncodedSize(encodedSize);
        setEdgeId(shuffleId.getOutEdgeId());
    }

    public ShuffleId getShuffleId() {
        return shuffleId;
    }

    public void setAddress(ShuffleAddress address) {
        this.address = address;
    }

    public ShuffleAddress getAddress() {
        return address;
    }

}
