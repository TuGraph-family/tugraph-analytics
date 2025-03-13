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

package com.antgroup.geaflow.dsl.connector.file;

import com.antgroup.geaflow.dsl.connector.api.TableConnector;
import com.antgroup.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectorFactoryTest {

    @Test
    public void testLoadConnector() {
        TableConnector connector = ConnectorFactory.loadConnector("file");
        Assert.assertEquals(connector.getType(), "FILE");
    }

}
