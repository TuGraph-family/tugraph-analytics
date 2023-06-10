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

package com.antgroup.geaflow.dsl.connector.api.util;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.connector.api.TableConnector;
import java.util.ServiceLoader;

public class ConnectorFactory {

    public static TableConnector loadConnector(String tableType) {
        ServiceLoader<TableConnector> connectors = ServiceLoader.load(TableConnector.class);
        TableConnector currentConnector = null;
        for (TableConnector connector : connectors) {
            if (connector.getType().equalsIgnoreCase(tableType)) {
                currentConnector = connector;
                break;
            }
        }
        if (currentConnector == null) {
            throw new GeaFlowDSLException("Table type: '{}' has not implement", tableType);
        }
        return currentConnector;
    }
}
