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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.socket.SocketConfigKeys;
import com.antgroup.geaflow.dsl.connector.socket.SocketTableConnector;
import com.antgroup.geaflow.dsl.connector.socket.SocketTableSource.SocketOffset;
import com.antgroup.geaflow.dsl.connector.socket.SocketTableSource.SocketPartition;
import com.antgroup.geaflow.dsl.connector.socket.server.NettyTerminalServer;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import java.util.ArrayList;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SocketTableConnectorTest {

    public NettyTerminalServer setup(int port) throws Exception {
        NettyTerminalServer nettyTerminalServer = new NettyTerminalServer();
        new Thread(() -> {
            nettyTerminalServer.bind(port);
        }).start();
        return nettyTerminalServer;
    }

    @Test
    public void testSocketReadWrite() throws Exception {
        setup(9003);
        SocketTableConnector connector = new SocketTableConnector();
        Assert.assertEquals(connector.getType(), "SOCKET");
        Configuration tableConf = new Configuration();
        tableConf.put(SocketConfigKeys.GEAFLOW_DSL_SOCKET_HOST.getKey(), "localhost");
        tableConf.put(SocketConfigKeys.GEAFLOW_DSL_SOCKET_PORT.getKey(), "9003");
        TableSource tableSource = connector.createSource(tableConf);
        Assert.assertEquals(tableSource.getDeserializer(tableConf).getClass(), TextDeserializer.class);
        TableSchema sourceSchema = new TableSchema(new TableField("text", StringType.INSTANCE, true));
        tableSource.init(tableConf, sourceSchema);
        tableSource.open(new DefaultRuntimeContext(tableConf));
        Assert.assertEquals(tableSource.listPartitions().size(), 1);
        Partition partition = tableSource.listPartitions().get(0);

        TableSink tableSink = connector.createSink(tableConf);

        TableSchema sinkSchema = new TableSchema(new TableField("id", IntegerType.INSTANCE, true),
            new TableField("name", StringType.INSTANCE, true));
        tableSink.init(tableConf, sinkSchema);
        tableSink.open(new DefaultRuntimeContext(tableConf));

        tableSink.write(ObjectRow.create(1, "jim"));
        tableSink.finish();
        tableSink.close();

        try {
            tableSource.fetch(partition, Optional.empty(), -1);
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), GeaFlowDSLException.class);
        }
        tableSource.close();
    }

    @Test
    public void testSocketPartitionAndOffset() {
        SocketPartition socketPartition1 = new SocketPartition(new ArrayList<>());
        SocketPartition socketPartition2 = new SocketPartition(new ArrayList<>());
        Assert.assertFalse(socketPartition1.equals(null));
        Assert.assertEquals(socketPartition1.hashCode(), socketPartition2.hashCode());
        Assert.assertEquals(socketPartition1, socketPartition1);
        Assert.assertEquals(socketPartition1, socketPartition2);
        Assert.assertEquals(socketPartition1.getData().size(), 0);
        Assert.assertEquals(socketPartition1.getName(), socketPartition2.getName());

        SocketOffset socketOffset = new SocketOffset();
        Assert.assertEquals(socketOffset.getOffset(), -1);
        Assert.assertEquals(socketOffset.humanReadable(), "None");
        Assert.assertFalse(socketOffset.isTimestamp());
    }
}
