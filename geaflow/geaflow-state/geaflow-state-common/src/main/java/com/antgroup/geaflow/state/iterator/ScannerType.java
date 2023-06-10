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

package com.antgroup.geaflow.state.iterator;

public enum ScannerType {
    /**
     * FULL SCANNER.
     */
    FULL(false, true),
    /**
     * FULL SCANNER WITH FILTER.
     */
    FULL_WITH_FILTER(false, false),
    /**
     * MULTI KEYS SCANNER.
     */
    KEYS(true, true),
    /**
     * MULTI KEYS SCANNER WITH FILTERS.
     */
    KEYS_WITH_FILTER(true, false);

    ScannerType(boolean isMultiKey, boolean emptyFilter) {
        this.isMultiKey = isMultiKey;
        this.emptyFilter = emptyFilter;
    }

    private final boolean isMultiKey;
    private final boolean emptyFilter;

    public boolean isMultiKey() {
        return isMultiKey;
    }

    public boolean emptyFilter() {
        return emptyFilter;
    }
}
