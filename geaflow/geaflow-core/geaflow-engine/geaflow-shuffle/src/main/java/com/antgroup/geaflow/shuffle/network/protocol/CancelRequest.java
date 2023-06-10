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
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;

/**
 * Message to notify producer to cancel.
 */
public class CancelRequest extends NettyMessage {

    final ChannelId receiverId;

    public CancelRequest(ChannelId receiverId) {
        this.receiverId = Preconditions.checkNotNull(receiverId);
    }

    public ChannelId receiverId()  {
        return receiverId;
    }

    @Override
    public ByteBuf write(ByteBufAllocator allocator) throws Exception {
        ByteBuf result = null;

        try {
            result = allocateBuffer(allocator, MessageType.CANCEL_CONNECTION.getId(), 16);
            receiverId.writeTo(result);
        } catch (Throwable t) {
            if (result != null) {
                result.release();
            }

            throw new IOException(t);
        }

        return result;
    }

    public static CancelRequest readFrom(ByteBuf buffer) throws Exception {
        return new CancelRequest(ChannelId.readFrom(buffer));
    }

}
