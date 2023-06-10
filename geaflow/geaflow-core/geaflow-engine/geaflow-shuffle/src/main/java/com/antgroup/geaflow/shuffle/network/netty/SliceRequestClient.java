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

import com.antgroup.geaflow.shuffle.api.pipeline.channel.RemoteInputChannel;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.network.ConnectionId;
import com.antgroup.geaflow.shuffle.network.protocol.BatchRequest;
import com.antgroup.geaflow.shuffle.network.protocol.CloseRequest;
import com.antgroup.geaflow.shuffle.network.protocol.SliceRequest;
import com.antgroup.geaflow.shuffle.util.AtomicReferenceCounter;
import com.antgroup.geaflow.shuffle.util.TransportException;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceRequestClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SliceRequestClient.class);

    private final Channel tcpChannel;
    private final ConnectionId connectionId;
    private final SliceRequestClientHandler clientHandler;
    private final SliceRequestClientFactory clientFactory;
    // If zero, the underlying TCP channel can be safely closed.
    private final AtomicReferenceCounter closeReferenceCounter = new AtomicReferenceCounter();

    public SliceRequestClient(Channel tcpChannel, SliceRequestClientHandler clientHandler,
                              ConnectionId connectionId, SliceRequestClientFactory clientFactory) {

        this.tcpChannel = Preconditions.checkNotNull(tcpChannel);
        this.clientHandler = Preconditions.checkNotNull(clientHandler);
        this.connectionId = Preconditions.checkNotNull(connectionId);
        this.clientFactory = Preconditions.checkNotNull(clientFactory);
    }

    public boolean disposeIfNotUsed() {
        return closeReferenceCounter.disposeIfNotUsed();
    }

    /**
     * Increments the reference counter.
     *
     * <p>Note: the reference counter has to be incremented before returning the
     * instance of this client to ensure correct closing logic.
     */
    boolean incrementReferenceCounter() {
        return closeReferenceCounter.increment();
    }

    /**
     * Requests a remote intermediate result partition queue.
     *
     * <p>The request goes to the remote producer, for which this partition
     * request client instance has been created.
     */
    public void requestSlice(SliceId sliceId, final RemoteInputChannel inputChannel, int delayMs,
                             long startBatchId) throws IOException {

        checkNotClosed();
        clientHandler.addInputChannel(inputChannel);

        final SliceRequest request = new SliceRequest(sliceId, startBatchId,
            inputChannel.getInputChannelId());

        final ChannelFutureListener listener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    clientHandler.removeInputChannel(inputChannel);
                    SocketAddress remoteAddr = future.channel().remoteAddress();
                    inputChannel.onError(new TransportException(
                        String.format("Sending the request to '%s' failed.", remoteAddr),
                        future.channel().localAddress(), future.cause()));
                }
            }
        };

        if (delayMs == 0) {
            ChannelFuture f = tcpChannel.writeAndFlush(request);
            f.addListener(listener);
        } else {
            final ChannelFuture[] f = new ChannelFuture[1];
            tcpChannel.eventLoop().schedule(new Runnable() {
                @Override
                public void run() {
                    f[0] = tcpChannel.writeAndFlush(request);
                    f[0].addListener(listener);
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    public void requestNextBatch(long batchId, final RemoteInputChannel inputChannel)
        throws IOException {

        checkNotClosed();

        final BatchRequest request = new BatchRequest(batchId, inputChannel.getInputChannelId());

        final ChannelFutureListener listener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    SocketAddress remoteAddr = future.channel().remoteAddress();
                    inputChannel.onError(new TransportException(
                        String.format("Sending the batch request to '%s' failed.", remoteAddr),
                        future.channel().localAddress(), future.cause()));
                }
            }
        };

        ChannelFuture f = tcpChannel.writeAndFlush(request);
        f.addListener(listener);
    }

    public void close(RemoteInputChannel inputChannel) throws IOException {
        clientHandler.removeInputChannel(inputChannel);

        if (closeReferenceCounter.decrement()) {
            // Close the TCP connection. Send a close request msg to ensure
            // that outstanding backwards task events are not discarded.
            tcpChannel.writeAndFlush(new CloseRequest())
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

            // Make sure to remove the client from the factory.
            clientFactory.destroyRequestClient(connectionId, this);
        } else {
            LOGGER.warn("cancel slice consumption of {}", inputChannel.getInputSliceId());
            clientHandler.cancelRequest(inputChannel.getInputChannelId());
        }
    }

    private void checkNotClosed() throws IOException {
        if (closeReferenceCounter.isDisposed()) {
            final SocketAddress localAddr = tcpChannel.localAddress();
            final SocketAddress remoteAddr = tcpChannel.remoteAddress();
            throw new TransportException(String.format("Channel to '%s' closed.", remoteAddr),
                localAddr);
        }
    }

}
