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

import io.netty.buffer.ByteBuf;
import java.io.Serializable;
import java.util.Objects;

public class SliceId implements Serializable {

    private final WriterId writerId;
    private final int sliceIndex;

    public SliceId(long pipelineId, int edgeId, int shardIndex, int sliceIndex) {
        this.writerId = new WriterId(pipelineId, edgeId, shardIndex);
        this.sliceIndex = sliceIndex;
    }

    public SliceId(WriterId writerId, int sliceIndex) {
        this.writerId = writerId;
        this.sliceIndex = sliceIndex;
    }

    public long getPipelineId() {
        return writerId.getPipelineId();
    }

    public int getEdgeId() {
        return writerId.getEdgeId();
    }

    public int getShardIndex() {
        return writerId.getShardIndex();
    }

    public int getSliceIndex() {
        return sliceIndex;
    }

    public WriterId getWriterId() {
        return writerId;
    }

    public void writeTo(ByteBuf buf) {
        buf.writeLong(writerId.getPipelineId());
        buf.writeInt(writerId.getEdgeId());
        buf.writeInt(writerId.getShardIndex());
        buf.writeInt(sliceIndex);
    }

    public static SliceId readFrom(ByteBuf buf) {
        long execId = buf.readLong();
        int edgeId = buf.readInt();
        int shardIndex = buf.readInt();
        int sliceIndex = buf.readInt();
        return new SliceId(execId, edgeId, shardIndex, sliceIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SliceId sliceId = (SliceId) o;
        return sliceIndex == sliceId.sliceIndex && Objects.equals(writerId, sliceId.writerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writerId, sliceIndex);
    }

    @Override
    public String toString() {
        return "SliceId{" + "pipelineId=" + writerId.getPipelineId() + ", edgeId=" + writerId
            .getEdgeId() + ", " + "shardIndex=" + writerId.getShardIndex() + ", sliceIndex="
            + sliceIndex + '}';
    }
}
