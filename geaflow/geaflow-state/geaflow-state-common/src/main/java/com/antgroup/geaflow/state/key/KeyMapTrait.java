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

package com.antgroup.geaflow.state.key;

import java.util.List;
import java.util.Map;

public interface KeyMapTrait<K, UK, UV> {

    /**
     * Returns the current value associated with the given key.
     */
    Map<UK, UV> get(K key);

    /**
     * Returns the current value associated with the given key.
     */
    List<UV> get(K key, UK... subKeys);

    /**
     * Associates a new value with the given key.
     */
    void add(K key, UK subKey, UV value);

    /**
     * Resets the state value.
     */
    void add(K key, Map<UK, UV> map);

    /**
     * Deletes the mapping of the given key.
     */
    void remove(K key);

    /**
     * Deletes the mapping of the given key.
     */
    void remove(K key, UK... subKeys);


}
