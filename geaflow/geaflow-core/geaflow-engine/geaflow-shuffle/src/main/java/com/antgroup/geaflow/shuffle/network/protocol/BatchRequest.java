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

import com.antgroup.geaflow.shuffle.pipeline.channel.ChannelId;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;

/**
 * Incremental request sequence id from the client to the server.
 */
public class BatchRequest extends NettyMessage {

    final long nextBatchId;
    final ChannelId receiverId;

    public BatchRequest(long nextBatchId, ChannelId receiverId) {
        Preconditions.checkArgument(nextBatchId >= 0, "The sequence id should be positive");
        this.nextBatchId = nextBatchId;
        this.receiverId = receiverId;
    }

    public ChannelId receiverId() {
        return receiverId;
    }

    public long getNextBatchId() {
        return nextBatchId;
    }

    @Override
    public ByteBuf write(ByteBufAllocator allocator) throws IOException {
        ByteBuf result = null;

        try {
            result = allocateBuffer(allocator,
                MessageType.FETCH_BATCH_REQUEST.getId(), 8 + 16);
            result.writeLong(nextBatchId);
            receiverId.writeTo(result);

            return result;
        } catch (Throwable t) {
            if (result != null) {
                result.release();
            }

            throw new IOException(t);
        }
    }

    public static BatchRequest readFrom(ByteBuf buffer) {
        long nextBatchId = buffer.readLong();
        ChannelId receiverId = ChannelId.readFrom(buffer);

        return new BatchRequest(nextBatchId, receiverId);
    }

    @Override
    public String toString() {
        return String.format("BatchRequest(%s: %d)", receiverId, nextBatchId);
    }

}
