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

package com.antgroup.geaflow.example.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.FileUtil;
import com.antgroup.geaflow.metaserver.MetaServer;
import com.antgroup.geaflow.metaserver.MetaServerContext;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import java.io.File;
import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.BeforeClass;

public class BaseServiceTest {

    protected TestingServer server;
    protected File testDir;
    protected MetaServer metaServer;

    protected Configuration configuration;

    public final String graphView =
        "CREATE GRAPH IF NOT EXISTS modern_2 (\n" +
            "\tVertex person (\n" +
            "\t  id bigint ID,\n" +
            "\t  name varchar,\n" +
            "\t  age int\n" +
            "\t),\n" +
            "\tVertex software (\n" +
            "\t  id bigint ID,\n" +
            "\t  name varchar,\n" +
            "\t  lang varchar\n" +
            "\t),\n" +
            "\tEdge knows (\n" +
            "\t  srcId bigint SOURCE ID,\n" +
            "\t  targetId bigint DESTINATION ID,\n" +
            "\t  weight double\n" +
            "\t),\n" +
            "\tEdge created (\n" +
            "\t  srcId bigint SOURCE ID,\n" +
            "\t  targetId bigint DESTINATION ID,\n" +
            "\t  weight double\n" +
            "\t)\n" +
            ") WITH (\n" +
                "\tstoreType='memory',\n" +
                "\tshardCount = 2\n" +
            ");\n" +
            "USE GRAPH modern_2;\n";

    public final String analyticsQuery = graphView + "MATCH (a) RETURN a limit 0";
    public final String executeQuery = graphView + "MATCH (a) RETURN a limit 1";

    @BeforeClass
    public void beforeClass() {
        String jobName = "test_analytics_" + System.currentTimeMillis();
        testDir = new File(FileUtil.constitutePath("tmp","zk",jobName));
        configuration = new Configuration();
        configuration.put("geaflow.zookeeper.znode.parent", File.separator + jobName);
        configuration.put("geaflow.zookeeper.quorum.servers", "localhost:2181");
    }

    public void before() throws Exception {
        if (testDir.exists()) {
            testDir.delete();
        }
        if (!testDir.exists()) {
            testDir.mkdir();
        }
        server = new TestingServer(2181, testDir);
        server.start();

        metaServer = new MetaServer();
        metaServer.init(new MetaServerContext(configuration));
    }

    public void after() throws IOException {
        metaServer.close();
        server.stop();
        testDir.delete();
    }

    protected static ManagedChannel buildChannel(String host, int port, int timeoutMs) {
        return NettyChannelBuilder.forAddress(host, port)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .build();
    }
}
