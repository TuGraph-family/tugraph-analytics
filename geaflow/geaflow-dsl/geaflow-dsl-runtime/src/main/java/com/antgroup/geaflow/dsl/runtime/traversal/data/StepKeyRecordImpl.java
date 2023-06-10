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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowKey;

public class StepKeyRecordImpl implements StepKeyRecord {

    private final RowKey key;

    private final Row value;

    public StepKeyRecordImpl(RowKey key, Row value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public RowKey getKey() {
        return key;
    }

    @Override
    public Row getValue() {
        return value;
    }
}
