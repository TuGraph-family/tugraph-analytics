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

package com.antgroup.geaflow.state.strategy.manager;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.state.action.hook.ScaleHook.ScaleManager;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ScaleManagerTest {

    @Test
    public void test() {
        FileUtils.deleteQuietly(new File("/tmp/localChk"));

        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "ScaleManagerTest");
        configuration.put(FileConfigKeys.ROOT.getKey(), "/tmp/localChk");
        ScaleManager scaleManager = new ScaleManager("test", configuration, 4);

        scaleManager.tryStoreShardNum(1L);
        scaleManager.tryStoreShardNum(2L);
        scaleManager.tryStoreShardNum(3L);

        Assert.assertFalse(scaleManager.needScale(3L));

        // recover 3
        scaleManager = new ScaleManager("test", configuration, 8);
        Assert.assertTrue(scaleManager.needScale(3L));

        Assert.assertEquals(scaleManager.getRecoverShardId(4), 0);
        Assert.assertEquals(scaleManager.getRecoverShardId(5), 1);
        scaleManager.tryStoreShardNum(3L);

        // recover 4
        scaleManager = new ScaleManager("test", configuration, 8);
        Assert.assertFalse(scaleManager.needScale(4L));
        scaleManager.tryStoreShardNum(4L);

        // recover 5
        scaleManager = new ScaleManager("test", configuration, 32);
        Assert.assertTrue(scaleManager.needScale(5L));
        Assert.assertEquals(scaleManager.getRecoverShardId(31), 7);
        Assert.assertEquals(scaleManager.getRecoverShardId(5), 5);


        FileUtils.deleteQuietly(new File("/tmp/localChk"));
    }
}
