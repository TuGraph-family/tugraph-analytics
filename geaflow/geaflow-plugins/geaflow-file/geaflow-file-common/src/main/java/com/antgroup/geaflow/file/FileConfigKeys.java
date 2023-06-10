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

package com.antgroup.geaflow.file;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class FileConfigKeys {

    protected static final int CORE_NUM = Runtime.getRuntime().availableProcessors();
    public static final ConfigKey PERSISTENT_TYPE = ConfigKeys
        .key("geaflow.file.persistent.type")
        .defaultValue("LOCAL")
        .description("geaflow file persistent type");

    public static final ConfigKey PERSISTENT_THREAD_SIZE = ConfigKeys
        .key("geaflow.file.persistent.thread.size")
        .defaultValue(CORE_NUM)
        .description("geaflow file persistent thread size");

    public static final ConfigKey USER_NAME = ConfigKeys
        .key("geaflow.file.persistent.user.name")
        .defaultValue("geaflow")
        .description("geaflow file base url");

    public static final ConfigKey JSON_CONFIG = ConfigKeys
        .key("geaflow.file.persistent.config.json")
        .defaultValue("")
        .description("geaflow json config");

    public static final ConfigKey ROOT = ConfigKeys
        .key("geaflow.file.persistent.root")
        .defaultValue("/geaflow/chk")
        .description("geaflow file persistent root path");

    /**
     * oss config.
     */
    public static final ConfigKey OSS_BUCKET_NAME = ConfigKeys
        .key("geaflow.file.oss.bucket.name")
        .defaultValue(null)
        .description("oss bucket name");

    public static final ConfigKey OSS_ENDPOINT = ConfigKeys
        .key("geaflow.file.oss.endpoint")
        .defaultValue(null)
        .description("oss endpoint");

    public static final ConfigKey OSS_ACCESS_ID = ConfigKeys
        .key("geaflow.file.oss.access.id")
        .defaultValue(null)
        .description("oss access id");

    public static final ConfigKey OSS_SECRET_KEY = ConfigKeys
        .key("geaflow.file.oss.secret.key")
        .defaultValue(null)
        .description("oss secret key");
}
