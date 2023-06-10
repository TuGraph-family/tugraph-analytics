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

package com.antgroup.geaflow.state.pushdown.inner;

import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.inner.PushDownPb.FilterNode;

/**
 * The filter converter is used to convert the user filter
 * to the filter recognized or optimized by the underground store.
 */
public interface IFilterConverter {

    /**
     * Returns the converted filter from the original filter.
     */
    IFilter convert(IFilter origin);

    /**
     * Returns the converted filter from the protobuf formatted filter.
     */
    IFilter convert(FilterNode filterNode);
}
