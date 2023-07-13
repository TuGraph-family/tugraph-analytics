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

package com.antgroup.geaflow.cluster.ray.failover;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import org.junit.Assert;
import org.testng.annotations.Test;

public class RayClusterFoStrategyTest {

    @Test
    public void testLoad() {
        Configuration configuration = new Configuration();
        IFailoverStrategy foStrategy = FailoverStrategyFactory.loadFailoverStrategy(EnvType.RAY_COMMUNITY, configuration.getString(FO_STRATEGY));
        Assert.assertNotNull(foStrategy);
        Assert.assertEquals(foStrategy.getType().name(), configuration.getString(FO_STRATEGY));

        IFailoverStrategy rayFoStrategy = FailoverStrategyFactory.loadFailoverStrategy(EnvType.RAY_COMMUNITY, "cluster_fo");
        Assert.assertNotNull(rayFoStrategy);
        Assert.assertEquals(rayFoStrategy.getClass(), RayClusterFailoverStrategy.class);

        rayFoStrategy = FailoverStrategyFactory.loadFailoverStrategy(EnvType.RAY_COMMUNITY, "component_fo");
        Assert.assertNotNull(rayFoStrategy);
        Assert.assertEquals(rayFoStrategy.getClass(), RayComponentFailoverStrategy.class);
    }

}
