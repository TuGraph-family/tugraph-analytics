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

import static com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType.SINGLE_VALUE;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.StepRecord;

public interface SingleValue extends StepRecord {

    default StepRecordType getType() {
        return SINGLE_VALUE;
    }

    Object getValue(IType<?> type);
}
