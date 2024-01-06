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

package com.antgroup.geaflow.state.jmh;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.JOB_MAX_PARALLEL;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class JMHParameter {

    @Param({"cstore", "rocksdb"})
    public String storeType;

    @Param({"1", "50"})
    public int outE;

    @Param({"10000"})
    public int vNum;

    public Configuration configuration = new Configuration(new HashMap<>(ImmutableMap.of(
        ExecutionConfigKeys.JOB_WORK_PATH.getKey(), "/tmp",
        FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL",
        FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/",
        JOB_MAX_PARALLEL.getKey(), "1"
    )));

}
