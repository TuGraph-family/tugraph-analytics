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

package com.antgroup.geaflow.shuffle.network.netty;

import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.network.ITransportContext;
import com.antgroup.geaflow.shuffle.network.protocol.NettyMessageDecoder;
import com.antgroup.geaflow.shuffle.network.protocol.NettyMessageEncoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NettyContext implements ITransportContext {

    private final NettyMessageEncoder messageEncoder = new NettyMessageEncoder();
    private final NettyMessageDecoder messageDecoder = new NettyMessageDecoder();
    private final ShuffleConfig config;

    public NettyContext(ShuffleConfig config) {
        this.config = config;
    }

    /**
     * Create the frame length decoder.
     * +------------------+------------------+--------++----------------+
     * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
     * +------------------+------------------+--------++----------------+
     *
     * @return decoder.
     */
    private ChannelHandler createFrameLengthDecoder() {
        if (config.enableCustomFrameDecoder()) {
            return new NettyFrameDecoder();
        }
        return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, -4, 4);
    }

    /**
     * Create channel handler on server for batch & pipeline shuffle.
     *
     * @return handlers.
     */
    @Override
    public ChannelHandler[] createServerChannelHandler(Channel channel) {
        SliceOutputChannelHandler queueOfPartitionQueue = new SliceOutputChannelHandler();
        SliceRequestServerHandler sliceRequestHandler = new SliceRequestServerHandler(
            queueOfPartitionQueue);

        return new ChannelHandler[]{messageEncoder, createFrameLengthDecoder(), messageDecoder,
            sliceRequestHandler, queueOfPartitionQueue};
    }

    /**
     * Create channel handler on client for pipeline shuffle.
     *
     * @return handlers.
     */
    public ChannelHandler[] createClientChannelHandlers(Channel channel) {
        SliceRequestClientHandler networkClientHandler = new SliceRequestClientHandler();

        return new ChannelHandler[]{messageEncoder, createFrameLengthDecoder(), messageDecoder,
            networkClientHandler};
    }

}
