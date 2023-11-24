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

package com.antgroup.geaflow.metaserver;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.FileUtil;
import com.antgroup.geaflow.metaserver.client.interal.MetaServerClient;
import com.antgroup.geaflow.metaserver.client.MetaServerQueryClient;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MetaServerTest {

    private TestingServer server;
    private File testDir;
    private MetaServer metaServer;

    private Configuration configuration;

    @BeforeClass
    public void before() throws Exception {
        String jobName = "test_zk" + System.currentTimeMillis();
        testDir = new File(FileUtil.constitutePath("tmp","zk",jobName));
        if (!testDir.exists()) {
            testDir.mkdir();
        }
        server = new TestingServer(2181, testDir);
        server.start();
        configuration = new Configuration();
        configuration.put("geaflow.zookeeper.znode.parent", File.separator + jobName);
        configuration.put("geaflow.zookeeper.quorum.servers", "localhost:2181");

        metaServer = new MetaServer();
        metaServer.init(new MetaServerContext(configuration));
    }

    @AfterClass
    public void after() throws IOException {
        metaServer.close();
        server.stop();
        testDir.delete();
    }


    @Test
    public void testRegister() {
        MetaServerClient client = MetaServerClient.getClient(configuration);

        client.registerService(NamespaceType.DEFAULT,"1", new HostAndPort("127.0.0.1", 1000));
        client.registerService(NamespaceType.DEFAULT,"2", new HostAndPort("127.0.0.1", 10242));
    }

    @Test(dependsOnMethods = "testRegister")
    public void testQueryService() {

        MetaServerQueryClient queryClient = MetaServerQueryClient.getClient(configuration);

        List<HostAndPort> serviceInfos = queryClient.queryAllServices(NamespaceType.DEFAULT);

        Assert.assertEquals(serviceInfos.size(), 2);
        Assert.assertTrue(serviceInfos.contains(new HostAndPort("127.0.0.1", 1000)));
        Assert.assertTrue(serviceInfos.contains(new HostAndPort("127.0.0.1", 10242)));
    }


}
