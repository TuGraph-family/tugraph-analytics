/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.runtime.testenv;

import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.function.GeaFlowTableSourceFunction;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import java.util.concurrent.ExecutorService;

public class SourceFunctionNoPartitionCheck extends GeaFlowTableSourceFunction {

    public SourceFunctionNoPartitionCheck(GeaFlowTable table, TableSource tableSource) {
        super(table, tableSource);
    }

    @Override
    protected ExecutorService startPartitionCompareThread() {
        return null;
    }
}
