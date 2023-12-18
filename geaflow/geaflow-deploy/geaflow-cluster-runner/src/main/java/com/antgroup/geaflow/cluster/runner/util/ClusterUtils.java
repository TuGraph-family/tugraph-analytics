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

package com.antgroup.geaflow.cluster.runner.util;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONFIG_FILE_LOG4J_NAME;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_START_COMMAND_TEMPLATE;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.JOB_CONFIG;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONF_DIR;

import com.antgroup.geaflow.cluster.config.ClusterJvmOptions;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.utils.JsonUtils;
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.StringUtil;

public class ClusterUtils {

    private static final String PROPERTY_FORMAT = "-D%s=\"%s\"";

    public static String getProperty(String key) {
        String value = System.getProperty(key);
        if (StringUtil.isEmpty(value)) {
            throw new GeaflowRuntimeException(String.format("Jvm property %s not found.", key));
        }
        return value;
    }

    public static String getEnvValue(Map<String, String> env, String envKey) {
        String value = env.get(envKey);
        Preconditions.checkArgument(value != null, "%s is not set", envKey);
        return value;
    }

    public static Configuration loadConfiguration() {
        String content = getProperty(JOB_CONFIG);
        return convertStringToConfig(content);
    }

    public static String convertConfigToString(Configuration configuration) {
        return StringEscapeUtils.escapeJava(JsonUtils.toJsonString(configuration.getConfigMap()));
    }

    public static Configuration convertStringToConfig(String content) {
        Map<String, String> map = JsonUtils.parseJson2map(content);
        return new Configuration(map);
    }

    public static String getStartCommand(ClusterJvmOptions jvmOpts, Class<?> mainClass,
                                         String logFilename, Configuration configuration,
                                         String classpath) {
        return getStartCommand(jvmOpts, mainClass, logFilename, configuration, null, classpath);
    }

    /**
     * This method is an adaptation of Flink's.
     * org.apache.flink.runtime.clusterframework.BootstrapTools#getTaskManagerShellCommand.
     */
    public static String getStartCommand(ClusterJvmOptions jvmOpts, Class<?> mainClass,
                                         String logFilename, Configuration configuration,
                                         Map<String, String> extraOpts, String classpath) {
        String confDir = configuration.getString(CONF_DIR);

        final Map<String, String> startCommandValues = new HashMap<>();
        startCommandValues.put("java", "$JAVA_HOME/bin/java");
        startCommandValues.put("classpath",
            "-classpath " + confDir + File.pathSeparator + classpath);
        startCommandValues.put("class", mainClass.getName());

        ArrayList<String> params = new ArrayList<>();
        params.add(String.format("-Xms%dm", jvmOpts.getXmsMB()));
        params.add(String.format("-Xmx%dm", jvmOpts.getMaxHeapMB()));
        if (jvmOpts.getXmnMB() > 0) {
            params.add(String.format("-Xmn%dm", jvmOpts.getXmnMB()));
        }
        if (jvmOpts.getMaxDirectMB() > 0) {
            params.add(String.format("-XX:MaxDirectMemorySize=%dm", jvmOpts.getMaxDirectMB()));
        }
        startCommandValues.put("jvmmem", StringUtils.join(params, ' '));
        List<String> opts = jvmOpts.getExtraOptions();
        if (extraOpts != null && !extraOpts.isEmpty()) {
            opts = new ArrayList<>(jvmOpts.getExtraOptions());
            for (Map.Entry<String, String> entry : extraOpts.entrySet()) {
                opts.add(String.format(PROPERTY_FORMAT, entry.getKey(), entry.getValue()));
            }
        }
        startCommandValues.put("jvmopts", StringUtils.join(opts, ' '));

        StringBuilder logging = new StringBuilder();
        logging.append("-Dlog.file=\"").append(logFilename).append("\"");
        logging.append(" -Dlog4j.configuration=" + CONFIG_FILE_LOG4J_NAME);

        startCommandValues.put("logging", logging.toString());
        String redirects = ">> " + logFilename + " 2>&1";
        startCommandValues.put("redirects", redirects);

        return getStartCommand(CONTAINER_START_COMMAND_TEMPLATE, startCommandValues);
    }

    public static String getStartCommand(String template, Map<String, String> startCommandValues) {
        for (Map.Entry<String, String> variable : startCommandValues.entrySet()) {
            template = template.replace("%" + variable.getKey() + "%", variable.getValue());
        }
        return template;
    }

}
