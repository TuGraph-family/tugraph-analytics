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

package com.antgroup.geaflow.shuffle.network.protocol;

import com.antgroup.geaflow.shuffle.api.pipeline.channel.ChannelId;
import com.antgroup.geaflow.shuffle.message.SliceId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;

public class SliceRequest extends NettyMessage {

    final SliceId sliceId;
    final long startBatchId;
    final ChannelId receiverId;

    public SliceRequest(SliceId sliceId, long startBatchId, ChannelId receiverId) {
        this.sliceId = sliceId;
        this.startBatchId = startBatchId;
        this.receiverId = receiverId;
    }

    public ChannelId getReceiverId() {
        return receiverId;
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public long getStartBatchId() {
        return startBatchId;
    }

    @Override
    public ByteBuf write(ByteBufAllocator allocator) throws IOException {
        ByteBuf result = null;

        try {
            result = allocateBuffer(allocator, MessageType.FETCH_SLICE_REQUEST.getId(),
                20 + 16 + 8);

            sliceId.writeTo(result);
            receiverId.writeTo(result);
            result.writeLong(startBatchId);

            return result;
        } catch (Throwable t) {
            if (result != null) {
                result.release();
            }
            throw new IOException(t);
        }
    }

    public static SliceRequest readFrom(ByteBuf buffer) {
        SliceId sliceId = SliceId.readFrom(buffer);
        ChannelId receiverId = ChannelId.readFrom(buffer);
        long startBatchId = buffer.readLong();

        return new SliceRequest(sliceId, startBatchId, receiverId);
    }

    @Override
    public String toString() {
        return String.format("SliceFetchRequest(%s, startBatchId=%s)", sliceId, startBatchId);
    }

}
