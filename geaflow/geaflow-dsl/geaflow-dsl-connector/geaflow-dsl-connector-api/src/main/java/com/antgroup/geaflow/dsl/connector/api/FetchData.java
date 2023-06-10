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

package com.antgroup.geaflow.dsl.connector.api;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The data fetched from the partition of {@link TableSource}.
 */
public class FetchData<T> implements Serializable {

    private final List<T> dataList;

    private final Offset nextOffset;

    private final boolean isFinish;

    public FetchData(List<T> data, Offset nextOffset, boolean isFinish) {
        this.dataList = Objects.requireNonNull(data);
        this.nextOffset = Objects.requireNonNull(nextOffset);
        this.isFinish = isFinish;
    }

    public FetchData(List<T> data, Offset nextOffset) {
        this(data, nextOffset, false);
    }

    /**
     * Returns data list.
     */
    public List<T> getDataList() {
        return dataList;
    }

    /**
     * Returns data size.
     */
    public int getDataSize() {
        return dataList.size();
    }

    /**
     * Returns the offset for next window.
     */
    public Offset getNextOffset() {
        return nextOffset;
    }

    /**
     * Returns true if the fetch has finished for the partition.
     */
    public boolean isFinish() {
        return isFinish;
    }
}
