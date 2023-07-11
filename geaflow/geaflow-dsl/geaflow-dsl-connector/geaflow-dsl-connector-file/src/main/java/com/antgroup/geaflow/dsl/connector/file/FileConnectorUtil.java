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

package com.antgroup.geaflow.dsl.connector.file;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConnectorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileConnectorUtil.class);

    private static final String HADOOP_HOME = "HADOOP_HOME";

    public static String getPartitionFileName(int taskIndex) {
        return "partition_" + taskIndex;
    }

    public static FileSystem getHdfsFileSystem(Configuration conf) {
        org.apache.hadoop.conf.Configuration hadoopConf = toHadoopConf(conf);
        FileSystem fileSystem;
        try {
            fileSystem = FileSystem.newInstance(hadoopConf);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Cannot init hdfs file system.", e);
        }
        return fileSystem;
    }

    public static org.apache.hadoop.conf.Configuration toHadoopConf(Configuration conf) {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        String hadoopConfPath = System.getenv(HADOOP_HOME);
        if (!StringUtils.isEmpty(hadoopConfPath)) {
            LOGGER.info("find hadoop home at: {}", hadoopConfPath);
            hadoopConf.addResource(new Path(hadoopConfPath + "/etc/hadoop/core-site.xml"));
            hadoopConf.addResource(new Path(hadoopConfPath + "/etc/hadoop/hdfs-site.xml"));
        }
        if (conf.contains(FileConfigKeys.JSON_CONFIG)) {
            String userConfigStr = conf.getString(FileConfigKeys.JSON_CONFIG);
            Map<String, String> userConfig = GsonUtil.parse(userConfigStr);
            if (userConfig != null) {
                for (Map.Entry<String, String> entry : userConfig.entrySet()) {
                    hadoopConf.set(entry.getKey(), entry.getValue());
                }
            }
        }
        return hadoopConf;
    }
}
