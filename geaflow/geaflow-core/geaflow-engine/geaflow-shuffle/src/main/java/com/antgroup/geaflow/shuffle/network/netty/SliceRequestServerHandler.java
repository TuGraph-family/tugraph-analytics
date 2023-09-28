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

import com.antgroup.geaflow.shuffle.api.pipeline.channel.ChannelId;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.SequenceSliceReader;
import com.antgroup.geaflow.shuffle.network.protocol.BatchRequest;
import com.antgroup.geaflow.shuffle.network.protocol.CancelRequest;
import com.antgroup.geaflow.shuffle.network.protocol.CloseRequest;
import com.antgroup.geaflow.shuffle.network.protocol.ErrorResponse;
import com.antgroup.geaflow.shuffle.network.protocol.NettyMessage;
import com.antgroup.geaflow.shuffle.network.protocol.SliceRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceRequestServerHandler.class);

    private final SliceOutputChannelHandler outboundQueue;

    public SliceRequestServerHandler(SliceOutputChannelHandler outboundQueue) {
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();
            if (msgClazz == SliceRequest.class) {
                SliceRequest request = (SliceRequest) msg;
                try {
                    SequenceSliceReader reader = new SequenceSliceReader(
                        request.getReceiverId(), outboundQueue);
                    reader.createSliceReader(request.getSliceId(), request.getStartBatchId());

                    outboundQueue.notifyReaderCreated(reader);
                } catch (Throwable notFound) {
                    respondWithError(ctx, notFound, request.getReceiverId());
                }
            } else if (msgClazz == CancelRequest.class) {
                CancelRequest request = (CancelRequest) msg;

                outboundQueue.cancel(request.receiverId());
            } else if (msgClazz == CloseRequest.class) {

                outboundQueue.close();
            } else if (msgClazz == BatchRequest.class) {
                BatchRequest request = (BatchRequest) msg;

                outboundQueue.updateRequestedBatchId(request.receiverId(),
                    reader -> reader.requestBatch(request.getNextBatchId()));
            } else {
                LOGGER.warn("Received unexpected client request: {}", msg);
                respondWithError(ctx, new IllegalArgumentException("unknown request:" + msg));
            }
        } catch (Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new ErrorResponse(error));
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error,
                                  ChannelId sourceId) {
        ctx.writeAndFlush(new ErrorResponse(sourceId, error));
    }

}
