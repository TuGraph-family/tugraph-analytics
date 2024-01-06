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

package com.antgroup.geaflow.analytics.service.client.jdbc.property;

import static java.lang.String.format;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Optional;
import java.util.Properties;

/**
 * This class is an adaptation of Presto's com.facebook.presto.jdbc.ConnectionProperty.
 */
public interface ConnectProperty<T> {

    String getPropertyKey();

    Optional<String> getDefault();

    boolean enableRequired(Properties properties);

    boolean enableAllowed(Properties properties);

    Optional<T> getValue(Properties properties) throws GeaflowRuntimeException;

    void validate(Properties properties) throws GeaflowRuntimeException;

    default T getRequiredValue(Properties properties) throws GeaflowRuntimeException {
        return getValue(properties).orElseThrow(() ->
            new GeaflowRuntimeException(format("Connect property '%s' is required", getPropertyKey())));
    }
}
