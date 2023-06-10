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

import java.util.Collection;

/**
 * The interface describe the state supporting modification.
 */
public interface RevisableState<K, R> {

    /**
     * add state method with data.
     */
    void add(R r);

    /**
     * update state method with data.
     */
    void update(R r);

    /**
     * delete state method with data.
     */
    void delete(R r);

    /**
     * delete state method with multi keys.
     */
    void delete(K... ids);

    /**
     * delete state method with multi keys.
     */
    void delete(Collection<K> ids);
}
