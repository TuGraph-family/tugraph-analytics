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

package com.antgroup.geaflow.state.strategy.manager;

import com.antgroup.geaflow.state.key.KeyListTrait;
import com.antgroup.geaflow.state.key.KeyMapTrait;
import com.antgroup.geaflow.state.key.KeyValueTrait;

public class KeyStateManager<K> extends BaseStateManager implements IKeyStateManager<K> {

    private KeyValueTrait keyValueTrait;
    private KeyListTrait keyListTrait;
    private KeyMapTrait keyMapTrait;

    @Override
    public <V> KeyValueTrait<K, V> getKeyValueTrait(Class<V> valueClazz) {
        if (this.keyValueTrait == null) {
            this.keyValueTrait = new KeyValueManagerImpl<>(context, accessorMap);
        }
        return this.keyValueTrait;
    }

    @Override
    public <V> KeyListTrait<K, V> getKeyListTrait(Class<V> valueClazz) {
        if (this.keyListTrait == null) {
            this.keyListTrait = new KeyListManagerImpl<>(context, accessorMap);
        }
        return this.keyListTrait;
    }

    @Override
    public <UK, UV> KeyMapTrait<K, UK, UV> getKeyMapTrait(Class<UK> subKeyClazz,
                                                          Class<UV> valueClazz) {
        if (this.keyMapTrait == null) {
            this.keyMapTrait = new KeyMapManagerImpl<>(context, accessorMap);
        }
        return this.keyMapTrait;
    }
}
