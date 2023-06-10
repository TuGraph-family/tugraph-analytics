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

package com.antgroup.geaflow.state.descriptor;

import com.antgroup.geaflow.state.serializer.IKeySerializer;

public abstract class BaseKeyDescriptor<K> extends BaseStateDescriptor {

    protected Class<K> keyClazz;
    protected IKeySerializer<K> keySerializer;

    protected BaseKeyDescriptor(String name, String storeType) {
        super(name, storeType);
    }

    public Class<K> getKeyClazz() {
        return keyClazz;
    }

    public IKeySerializer<K> getKeySerializer() {
        return keySerializer;
    }
}
