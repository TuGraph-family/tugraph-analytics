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

package com.antgroup.geaflow.common.config.keys;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;
import java.io.Serializable;

public class FrameworkConfigKeys implements Serializable {

    public static final ConfigKey ENABLE_EXTRA_OPTIMIZE = ConfigKeys
        .key("geaflow.extra.optimize.enable")
        .defaultValue(false)
        .description("union optimization, disabled by default");

    public static final ConfigKey ENABLE_EXTRA_OPTIMIZE_SINK = ConfigKeys
        .key("geaflow.extra.optimize.sink.enable")
        .defaultValue(false)
        .description("union optimization starts on the sink, disabled by default");

    public static final ConfigKey JOB_MAX_PARALLEL = ConfigKeys
        .key("geaflow.job.max.parallel")
        .defaultValue(1024)
        .description("maximum parallelism of the job, default value is 1024");

    public static final ConfigKey STREAMING_RUN_TIMES = ConfigKeys
        .key("geaflow.streaming.job.run.times")
        .defaultValue(Long.MAX_VALUE)
        .description("maximum number of job runs");

    public static final ConfigKey STREAMING_FLYING_BATCH_NUM = ConfigKeys
        .key("geaflow.streaming.flying.batch.num")
        .defaultValue(5)
        .description("the number of batches that pipelined job runs simultaneously, default value is 5");

    public static final ConfigKey BATCH_NUMBER_PER_CHECKPOINT = ConfigKeys
        .key("geaflow.batch.number.per.checkpoint")
        .defaultValue(5L)
        .description("do checkpoint every specified number of batch");

    public static final ConfigKey SYSTEM_STATE_BACKEND_TYPE = ConfigKeys
        .key("geaflow.system.state.backend.type")
        .defaultValue("ROCKSDB")
        .description("system state backend store type, e.g., [rocksdb, memory]");

    public static final ConfigKey SYSTEM_OFFSET_BACKEND_TYPE = ConfigKeys
        .key("geaflow.system.offset.backend.type")
        .defaultValue("MEMORY")
        .description("system offset backend store type, e.g., [jdbc, memory]");

    public static final ConfigKey INC_STREAM_MATERIALIZE_DISABLE = ConfigKeys
        .key("geaflow.inc.stream.materialize.disable")
        .defaultValue(false)
        .description("inc stream materialize, enabled by default");
}

