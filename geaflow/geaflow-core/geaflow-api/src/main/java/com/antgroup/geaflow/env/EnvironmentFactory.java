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

package com.antgroup.geaflow.env;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.env.args.EnvironmentArgumentParser;
import com.antgroup.geaflow.env.args.IEnvironmentArgsParser;
import com.antgroup.geaflow.state.StoreType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentFactory.class);

    public static Environment onLocalEnvironment() {
        return onLocalEnvironment(new String[]{});
    }

    public static Environment onLocalEnvironment(String[] args) {
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        Map<String, String> config = new HashMap<>(argsParser.parse(args));
        config.put(RUN_LOCAL_MODE.getKey(), Boolean.TRUE.toString());
        // Set default state backend type to memory on local env.
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        if (!config.containsKey(SYSTEM_OFFSET_BACKEND_TYPE.getKey())) {
            config.put(SYSTEM_OFFSET_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        }
        Environment environment = (Environment) loadEnvironment(EnvType.LOCAL);
        environment.getEnvironmentContext().withConfig(config);
        return environment;
    }

    public static Environment onRayCommunityEnvironment() {
        return (Environment) loadEnvironment(EnvType.RAY_COMMUNITY);
    }

    public static Environment onRayCommunityEnvironment(String[] args) {
        Environment environment = (Environment) loadEnvironment(EnvType.RAY_COMMUNITY);
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        environment.getEnvironmentContext().withConfig(argsParser.parse(args));
        return environment;
    }

    public static Environment onK8SEnvironment() {
        return (Environment) loadEnvironment(EnvType.K8S);
    }

    public static Environment onK8SEnvironment(String[] args) {
        Environment environment = (Environment) loadEnvironment(EnvType.K8S);
        IEnvironmentArgsParser argsParser = loadEnvironmentArgsParser();
        environment.getEnvironmentContext().withConfig(argsParser.parse(args));
        return environment;
    }

    private static IEnvironment loadEnvironment(EnvType envType) {
        ServiceLoader<IEnvironment> contextLoader = ServiceLoader.load(IEnvironment.class);
        Iterator<IEnvironment> contextIterable = contextLoader.iterator();
        while (contextIterable.hasNext()) {
            IEnvironment environment = contextIterable.next();
            if (environment.getEnvType() == envType) {
                LOGGER.info("loaded IEnvironment implementation {}", environment);
                return environment;
            }
        }
        LOGGER.error("NOT found IEnvironment implementation with type:{}", envType);
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IEnvironment.class.getSimpleName()));
    }

    private static IEnvironmentArgsParser loadEnvironmentArgsParser() {
        ServiceLoader<IEnvironmentArgsParser> contextLoader = ServiceLoader.load(IEnvironmentArgsParser.class);
        Iterator<IEnvironmentArgsParser> contextIterable = contextLoader.iterator();
        boolean hasNext = contextIterable.hasNext();
        IEnvironmentArgsParser argsParser;
        if (hasNext) {
            argsParser = contextIterable.next();
        } else {
            // Use default argument parser.
            argsParser = new EnvironmentArgumentParser();
        }
        LOGGER.info("loaded IEnvironmentArgsParser implementation {}", argsParser);
        return argsParser;
    }
}
