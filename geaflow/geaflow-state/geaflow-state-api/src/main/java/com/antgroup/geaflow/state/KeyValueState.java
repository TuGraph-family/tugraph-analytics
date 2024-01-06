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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.state.key.KeyValueTrait;

/**
 * The key to value state interface.
 * The interface is inspired by the Flink's ValueState.
 */
public interface KeyValueState<K, V> extends KeyValueTrait<K, V>, IState {

    /**
     * Get the value.
     */
    V get(K key);

    /**
     * Update the value.
     */
    void put(K key, V value);

    /**
     * Remove key.
     */
    void remove(K key);

}
