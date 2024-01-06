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

package com.antgroup.geaflow.cluster.failover;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import org.testng.annotations.Test;

public class FoStrategyFactoryTest {

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testLoad() {
        FailoverStrategyFactory.loadFailoverStrategy(EnvType.RAY_COMMUNITY, "");
    }

}
