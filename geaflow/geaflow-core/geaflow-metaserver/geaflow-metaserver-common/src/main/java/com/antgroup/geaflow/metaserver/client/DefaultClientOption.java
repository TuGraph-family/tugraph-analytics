package com.antgroup.geaflow.metaserver.client;

import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;

public class DefaultClientOption {

    private static RpcClientOptions clientOption;

    public static synchronized RpcClientOptions build() {
        if (clientOption == null) {
            clientOption = new RpcClientOptions();
            clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
            int timeout = 100000;
            clientOption.setWriteTimeoutMillis(timeout);
            clientOption.setReadTimeoutMillis(timeout);
            clientOption.setConnectTimeoutMillis(2 * timeout);
            clientOption.setMaxTotalConnections(2);
            clientOption.setMinIdleConnections(2);
            clientOption.setWorkThreadNum(2);
            clientOption.setIoThreadNum(1);
            clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
            clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
            clientOption.setChannelType(ChannelType.POOLED_CONNECTION);
        }
        return clientOption;
    }
}
