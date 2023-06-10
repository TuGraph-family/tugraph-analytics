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

package com.antgroup.geaflow.model.record;

import com.antgroup.geaflow.model.record.impl.KeyRecord;
import com.antgroup.geaflow.model.record.impl.Record;
import java.io.Serializable;

public class RecordFactory implements Serializable {

    public static <T> IRecord<T> buildRecord(T value) {
        return new Record<>(value);
    }

    public static <KEY, T> IKeyRecord<KEY, T> buildRecord(KEY key, T value) {
        return new KeyRecord<>(key, value);
    }

}
