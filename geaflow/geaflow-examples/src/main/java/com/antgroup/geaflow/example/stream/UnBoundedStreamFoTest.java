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

package com.antgroup.geaflow.example.stream;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnBoundedStreamFoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnBoundedStreamFoTest.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Environment environment = EnvironmentFactory.onRayCommunityEnvironment(args);
        Configuration configuration = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
        Map<String, String> hbaseConfig = new HashMap<>();
        hbaseConfig.put("hbase.zookeeper.quorum", "hbase-v51-nn-10001.eu95sqa.tbsite"
            + ".net,hbase-v51-nn-10002.eu95sqa.tbsite.net,hbase-v51-nn-10003.eu95sqa.tbsite.net");
        hbaseConfig.put("zookeeper.znode.parent", "/hbase-eu95sqa-perf-test-ssd");
        hbaseConfig.put("hbase.zookeeper.property.clientPort", "2181");
        Map<String, String> config  = new HashMap<>();
        config.put("geaflow.store.hbase.config.json", GsonUtil.toJson(hbaseConfig));

        configuration.putAll(config);
        pipeline.submit(environment);
        SleepUtils.sleepSecond(3);
        LOGGER.error("main finished");
    }
}
