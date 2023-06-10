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

package com.antgroup.geaflow.dsl.runtime.engine.source;

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public abstract class AbstractFileTableSource<R extends Row> implements SourceFunction<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileTableSource.class);

    private static final String RESOURCE_HEAD = "resource://";

    private final String path;

    private final String separator;

    private BufferedReader reader;

    private boolean isClosed;

    private boolean finished = false;

    public AbstractFileTableSource(Configuration config) {
        this.path = config.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH);
        this.separator = config.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
    }

    @Override
    public void init(int parallel, int index) {
        try {
            if (path.startsWith(RESOURCE_HEAD)) {
                URL url = getClass().getResource(path.substring(RESOURCE_HEAD.length()));
                reader = new BufferedReader(new InputStreamReader(url.openStream()));
            } else {
                FileInputStream inputStream = new FileInputStream(path);
                reader = new BufferedReader(new InputStreamReader(inputStream));
            }
            this.isClosed = false;
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in open file: " + path);
        }
    }

    @Override
    public boolean fetch(IWindow<R> window, SourceContext<R> ctx) throws Exception {
        if (isClosed || finished) {
            return false;
        }
        String line;
        while ((line = reader.readLine()) != null) {
            if (StringUtils.isBlank(line)) {
                continue;
            }
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, separator);

            List<R> records = convert(fields);
            for (R r : records) {
                ctx.collect(r);
            }
        }
        finished = true;
        return false;
    }

    protected abstract List<R> convert(String[] fields);

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
                this.isClosed = true;
            } catch (IOException e) {
                LOGGER.warn("Error in close reader, just ignore", e);
            }
        }
    }

    public static Configuration replaceKey(Configuration config, ConfigKey oldKey, ConfigKey newKey) {
        Configuration newConfig = new Configuration(new HashMap<>(config.getConfigMap()));
        String oldValue = newConfig.getConfigMap().getOrDefault(oldKey.getKey(),
            String.valueOf(oldKey.getDefaultValue()));
        newConfig.put(newKey, oldValue);
        return newConfig;
    }
}
