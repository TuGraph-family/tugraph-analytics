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

package com.antgroup.geaflow.cluster.k8s.utils;

import static com.antgroup.geaflow.cluster.constants.AgentConstants.AGENT_LOG_SUFFIX;
import static com.antgroup.geaflow.cluster.constants.AgentConstants.ASYNC_PROFILER_SHELL_PATH;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.MASTER_ADDRESS;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.ADDRESS_SEPARATOR;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.CONFIG_KV_SEPARATOR;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.CONFIG_LIST_SEPARATOR;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.DRIVER_SERVICE_NAME_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONF_DIR;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_START_COMMAND_TEMPLATE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.LOG_DIR;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.USE_IP_IN_HOST_NETWORK;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.WORK_DIR;

import com.antgroup.geaflow.cluster.config.ClusterJvmOptions;
import com.antgroup.geaflow.cluster.constants.AgentConstants;
import com.antgroup.geaflow.cluster.k8s.config.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.dashboard.agent.runner.AgentWebRunner;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Toleration;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common utils for Kubernetes.
 */
public class KubernetesUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesUtils.class);

    private static InetAddress resolveServiceAddress(String serviceName) {
        try {
            return InetAddress.getByName(serviceName);
        } catch (UnknownHostException e) {
            return null;
        }
    }

    /**
     * This method is an adaptation of Flink's.
     * org.apache.flink.runtime.clusterframework.BootstrapTools#getStartCommand.
     */
    public static String getContainerStartCommand(String template,
                                                  Map<String, String> startCommandValues) {
        for (Map.Entry<String, String> variable : startCommandValues.entrySet()) {
            template = template.replace("%" + variable.getKey() + "%", variable.getValue());
        }
        return template;
    }

    public static Map<String, String> getPairsConf(Configuration config, ConfigKey configKey) {
        return getPairsConf(config, configKey.getKey());
    }

    public static Map<String, String> getPairsConf(Configuration config, String configKey) {
        Map<String, String> pairs = new HashMap<>();
        String pairsStr = config.getString(configKey);
        if (pairsStr != null) {
            for (String label : pairsStr.split(",")) {
                String[] splits = label.split(":");
                if (splits.length == 2) {
                    pairs.put(splits[0], splits[1]);
                }
            }
        }
        return pairs;
    }

    public static List<HostAlias> getHostAliases(ConfigMap configMap) {
        String hostAliases = configMap.getData().get(K8SConstants.HOST_ALIASES_CONFIG_MAP_NAME);
        List<HostAlias> hostAliasesList = new ArrayList<>();
        if (hostAliases != null) {
            for (String item : hostAliases.split("\n")) {
                if (item.startsWith("#")) {
                    continue;
                }
                String[] splits = item.split("\\s+");
                if (splits.length >= 2) {
                    List<String> hostNames = new ArrayList<>();
                    for (int i = 1; i < splits.length; i++) {
                        hostNames.add(splits[i].toLowerCase());
                    }
                    hostAliasesList.add(new HostAlias(hostNames, splits[0]));
                }
            }
        }
        return hostAliasesList;
    }

    public static Map<String, String> loadConfigurationFromString(String content) {
        Map<String, String> config = new HashMap<>();
        for (String line : content.split(System.lineSeparator())) {
            String[] splits = line.split(":");
            if (splits.length >= 2) {
                config.put(splits[0].trim(), StringUtils.substringAfter(line, ":").trim());
            }
        }
        return config;
    }

    /**
     * Method to extract variables from the config based on the given prefix String.
     * @param prefix Prefix for the variables key
     * @param config The config to get the environment variable defintion from
     */
    public static Map<String, String> getVariablesWithPrefix(String prefix,
                                                             Map<String, String> config) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                // remove prefix
                String key = entry.getKey().substring(prefix.length());
                result.put(key, entry.getValue());
            }
        }
        return result;
    }

    public static Configuration loadConfiguration() throws Exception {
        Configuration config = loadConfigurationFromFile();

        KubernetesConfig.DockerNetworkType dockerNetworkType =
            KubernetesConfig.getDockerNetworkType(
            config);

        // Wait for service to be resolved.
        String serviceIp = waitForServiceNameResolved(config, false).getHostAddress();
        config.put(MASTER_ADDRESS, serviceIp);
        if (dockerNetworkType == KubernetesConfig.DockerNetworkType.HOST) {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                if (config.getBoolean(USE_IP_IN_HOST_NETWORK)) {
                    config.put(MASTER_ADDRESS, serviceIp);
                } else {
                    config.put(MASTER_ADDRESS, addr.getHostName());
                }
            } catch (UnknownHostException e) {
                LOGGER.warn("Get hostname for master error {}.", e.getMessage());
            }
        }
        return config;
    }

    public static Configuration loadConfigurationFromFile() throws IOException {
        String configDir = System.getenv().get(K8SConstants.ENV_CONF_DIR);
        if (configDir == null) {
            throw new IllegalArgumentException(
                "Given configuration directory is null, cannot " + "load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new RuntimeException(
                "The given configuration directory name '" + configDir + "' ("
                    + confDirFile.getAbsolutePath() + ") does not describe an existing directory.");
        }

        // get yaml configuration file
        final File yamlConfigFile = new File(confDirFile, K8SConstants.ENV_CONFIG_FILE);

        if (!yamlConfigFile.exists()) {
            throw new IOException(
                "The config file '" + yamlConfigFile + "' (" + yamlConfigFile.getAbsolutePath()
                    + ") does not exist.");
        }

        return loadYAMLResource(yamlConfigFile);
    }

    /**
     * This method is an adaptation of Flink's
     * org.apache.flink.configuration.GlobalConfiguration#loadYAMLResource
     */
    @VisibleForTesting
    public static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String key;
                    String value;

                    String[] kv = conf.split(CONFIG_KV_SEPARATOR, 2);
                    if (kv.length < 1) {
                        LOGGER.warn(
                            "Error while trying to split key and value in configuration file "
                                + file + ":" + lineNo + ": \"" + line + "\"");
                        continue;
                    }

                    key = kv[0].trim();
                    value = kv.length == 1 ? "" : kv[1].trim();

                    // sanity check
                    if (key.length() == 0) {
                        LOGGER.warn(
                            "Error after splitting key in configuration file " + file + ":" + lineNo
                                + ": \"" + line + "\"");
                        continue;
                    }

                    LOGGER.info("Loading property: {}, {}", key, value);
                    config.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }

    public static InetAddress waitForServiceNameResolved(Configuration config,
                                                         boolean appendSuffix) {
        String serviceNameWithNamespace = KubernetesConfig.getServiceNameWithNamespace(config);
        String suffix = config.getString(SERVICE_SUFFIX);
        if (appendSuffix && !StringUtils.isBlank(suffix)) {
            serviceNameWithNamespace += K8SConstants.NAMESPACE_SEPARATOR + suffix;
        }
        LOGGER.info("Waiting for service {} to be resolved.", serviceNameWithNamespace);

        InetAddress serviceAddress;
        final long startTime = System.currentTimeMillis();
        do {
            serviceAddress = resolveServiceAddress(serviceNameWithNamespace);
            if (System.currentTimeMillis() - startTime > 60000) {
                LOGGER.warn("Resolve service took more than 60 seconds, please check logs on the "
                    + "Kubernetes cluster.");
            }
            SleepUtils.sleepMilliSecond(250);
        } while (serviceAddress == null);

        LOGGER.info("Service {} resolved to {}", serviceNameWithNamespace, serviceAddress);
        return serviceAddress;
    }

    public static String getEnvValue(Map<String, String> env, String envKey) {
        // Infer the resource identifier from the environment variable
        String value = env.get(envKey);
        Preconditions.checkArgument(value != null, "%s is not set", envKey);
        return value;
    }

    public static List<Toleration> getTolerations(Configuration config) {
        List<Toleration> tolerationList = new ArrayList<>();
        if (!config.contains(KubernetesConfigKeys.TOLERATION_LIST)) {
            return tolerationList;
        }
        String tolerations = config.getString(KubernetesConfigKeys.TOLERATION_LIST);
        for (String each : tolerations.trim().split(",")) {
            String[] parts = each.split(":", -1);
            if (parts.length != 5) {
                LOGGER.error("parse toleration error, {}", each);
                continue;
            }
            Toleration toleration = new Toleration();
            if (parts[0] != null && !parts[0].isEmpty() && !parts[0].equals("-")) {
                toleration.setKey(parts[0]);
            }
            if (parts[1] != null && !parts[1].isEmpty() && !parts[1].equals("-")) {
                toleration.setOperator(parts[1]);
            }
            if (parts[2] != null && !parts[2].isEmpty() && !parts[2].equals("-")) {
                toleration.setValue(parts[2]);
            }
            if (parts[3] != null && !parts[3].isEmpty() && !parts[3].equals("-")) {
                toleration.setEffect(parts[3]);
            }
            if (parts[4] != null && !parts[4].isEmpty() && !parts[4].equals("-")) {
                toleration.setTolerationSeconds(Long.valueOf(parts[4]));
            }
            tolerationList.add(toleration);
        }
        return tolerationList;
    }

    public static List<NodeSelectorRequirement> getMatchExpressions(Configuration config) {
        List<NodeSelectorRequirement> matchExpressionList = new ArrayList<>();
        if (!config.contains(KubernetesConfigKeys.MATCH_EXPRESSION_LIST)) {
            return matchExpressionList;
        }
        String matchExpressions = config.getString(KubernetesConfigKeys.MATCH_EXPRESSION_LIST);
        for (String each : matchExpressions.trim().split(",")) {
            String[] parts = each.split(":", -1);
            if (parts.length != 3) {
                LOGGER.error("parse matchExpressions error, {}", each);
                continue;
            }
            NodeSelectorRequirement matchExpression = new NodeSelectorRequirement();
            if (parts[0] != null && !parts[0].isEmpty() && !parts[0].equals("-")) {
                matchExpression.setKey(parts[0]);
            }
            if (parts[1] != null && !parts[1].isEmpty() && !parts[1].equals("-")) {
                matchExpression.setOperator(parts[1]);
            }
            if (parts[2] != null && !parts[2].isEmpty() && !parts[2].equals("-")) {
                matchExpression.setValues(Arrays.asList(parts[2]));
            }
            matchExpressionList.add(matchExpression);
        }
        return matchExpressionList;
    }

    @Nullable
    public static String extractComponentId(Pod pod) {
        return pod.getMetadata().getLabels().get(K8SConstants.LABEL_COMPONENT_ID_KEY);
    }

    public static String encodeRpcAddressMap(Map<String, ?> addressMap) {
        return Joiner.on(CONFIG_LIST_SEPARATOR).withKeyValueSeparator(ADDRESS_SEPARATOR)
            .join(addressMap);
    }

    public static Map<String, RpcAddress> decodeRpcAddressMap(String str) {
        Map<String, RpcAddress> map = new HashMap<>();
        for (String entry : str.trim().split(CONFIG_LIST_SEPARATOR)) {
            String[] pair = entry.split(ADDRESS_SEPARATOR);
            map.put(pair[0], RpcAddress.build(pair[1]));
        }
        return map;
    }

    public static String getMasterServiceName(String clusterId) {
        return clusterId + K8SConstants.SERVICE_NAME_SUFFIX;
    }

    public static String getMasterClientServiceName(String clusterId) {
        return clusterId + K8SConstants.CLIENT_SERVICE_NAME_SUFFIX;
    }

    public static String getDriverServiceName(String clusterId, int driverIndex) {
        return clusterId + DRIVER_SERVICE_NAME_SUFFIX + driverIndex;
    }

    /**
     * This method is an adaptation of Flink's.
     * org.apache.flink.runtime.clusterframework.BootstrapTools#getTaskManagerShellCommand.
     */
    public static String getContainerStartCommand(ClusterJvmOptions jvmOpts, Class<?> mainClass,
                                                  String logFilename, Configuration configuration) {
        String confDir = configuration.getString(CONF_DIR);

        final Map<String, String> startCommandValues = new HashMap<>();
        startCommandValues.put("java", "$JAVA_HOME/bin/java");
        startCommandValues.put("classpath", "-classpath " + confDir + File.pathSeparator + "$"
            + K8SConstants.ENV_GEAFLOW_CLASSPATH);
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
        startCommandValues.put("jvmopts", StringUtils.join(jvmOpts.getExtraOptions(), ' '));

        StringBuilder logging = new StringBuilder();
        logging.append("-Dlog.file=\"").append(logFilename).append("\"");
        logging.append(" -Dlog4j.configuration=" + K8SConstants.CONFIG_FILE_LOG4J_NAME);

        startCommandValues.put("logging", logging.toString());
        startCommandValues.put("redirects", ">> " + logFilename + " 2>&1");

        String commandTemplate = configuration.getString(CONTAINER_START_COMMAND_TEMPLATE);
        return getContainerStartCommand(commandTemplate, startCommandValues);
    }

    public static String getAgentShellCommand(Configuration config) {
        String agentPort = config.getString(ExecutionConfigKeys.AGENT_HTTP_PORT);
        String workDir = config.getString(WORK_DIR);
        String logDir = config.getString(LOG_DIR);
        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put(AgentConstants.AGENT_SERVER_PORT_KEY, agentPort);
        extraOptions.put(AgentConstants.AGENT_TMP_DIR_KEY, workDir);
        extraOptions.put(AgentConstants.LOG_DIR_KEY, logDir);
        extraOptions.put(AgentConstants.FLAME_GRAPH_PROFILER_PATH_KEY, ASYNC_PROFILER_SHELL_PATH);
        String jvmArgs = config.getString(ExecutionConfigKeys.AGENT_JVM_OPTIONS);
        ClusterJvmOptions jvmOpts = ClusterJvmOptions.build(jvmArgs);
        extraOptions.forEach((key, value) -> jvmOpts.getExtraOptions().add(String.format("-D%s=%s", key, value)));
        String logFileName = logDir + File.separator + AGENT_LOG_SUFFIX;
        return KubernetesUtils.getContainerStartCommand(jvmOpts,
            AgentWebRunner.class, logFileName, config);
    }

}
