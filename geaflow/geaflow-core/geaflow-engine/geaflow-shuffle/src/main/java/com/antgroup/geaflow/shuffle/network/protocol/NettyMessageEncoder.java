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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.io.IOException;

@ChannelHandler.Sharable
public class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
        if (msg instanceof NettyMessage) {
            Object serialized = null;
            try {
                serialized = ((NettyMessage) msg).write(ctx.alloc());
            } catch (Throwable t) {
                throw new IOException("Error while serializing message: " + msg, t);
            } finally {
                if (serialized != null) {
                    ctx.write(serialized, promise);
                }
            }
        } else {
            ctx.write(msg, promise);
        }
    }

}
