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

package com.antgroup.geaflow.state.pushdown.filter;

import com.antgroup.geaflow.state.data.DataType;
import java.util.List;

public class AndFilter extends BaseLogicFilter {

    public AndFilter(List<IFilter> inputs) {
        super(inputs);
    }

    public AndFilter(IFilter... inputs) {
        super(inputs);
    }

    void sanityCheck(IFilter filter) {
        DataType filterDataType = filter.dateType();
        if (this.dateType == null) {
            this.dateType = filterDataType;
        } else if (this.dateType != filterDataType) {
            this.dateType = DataType.VE;
        }

        FilterType filterType = filter.getFilterType();
        if (filterType == FilterType.AND || filterType == FilterType.OR) {
            this.rootBitSet.or(((BaseLogicFilter)filter).rootBitSet);
        } else if (filter.getFilterType().isRootFilter()) {
            this.rootBitSet.set(filter.getFilterType().ordinal());
        }
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.AND;
    }
}
