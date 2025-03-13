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

package com.antgroup.geaflow.operator.impl.window;

import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.model.common.Null;
import com.antgroup.geaflow.operator.base.window.AbstractOneInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeySelectorOperator<IN, KEY> extends
        AbstractOneInputOperator<IN, KeySelector<IN, KEY>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeySelectorOperator.class);

    public KeySelectorOperator(KeySelector<IN, KEY> keySelector) {
        super(keySelector);
    }

    @Override
    protected void process(IN value) throws Exception {
        KEY key = function.getKey(value);
        if (key == null) {
            key = (KEY) new Null();
        }
        collectKValue(key, value);
    }
}
