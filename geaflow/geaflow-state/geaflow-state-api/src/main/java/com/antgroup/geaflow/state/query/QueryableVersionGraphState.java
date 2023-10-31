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

package com.antgroup.geaflow.state.query;

import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.Map;

/**
 * The dynamic graph query interface.
 */
public interface QueryableVersionGraphState<K, VV, EV, R> {

    /**
     * Query by a filter, sharing by the search keys.
     */
    QueryableVersionGraphState<K, VV, EV, R> by(IFilter filter);

    /**
     * Query result is a map, which key is the version.
     */
    Map<Long, R> asMap();
}
