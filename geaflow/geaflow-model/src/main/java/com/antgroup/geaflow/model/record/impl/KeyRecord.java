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

package com.antgroup.geaflow.model.record.impl;

import com.antgroup.geaflow.model.record.IKeyRecord;

public class KeyRecord<KEY, T> extends Record<T> implements IKeyRecord<KEY, T> {

    protected KEY key;

    public KeyRecord(KEY key, Record<T> record) {
        super(record.getValue());
        this.key = key;
    }

    public KeyRecord(KEY key, T value) {
        super(value);
        this.key = key;
    }

    @Override
    public KEY getKey() {
        return key;
    }

    public void setKey(KEY key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return String.format("key:%s value:%s",key,super.toString());
    }
}
