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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * The data fetched from the partition of {@link TableSource}.
 */
public class FetchData<T> implements Serializable {

    private final Iterator<T> dataIterator;

    private final int size;

    private final Offset nextOffset;

    private final boolean isFinish;

    private FetchData(Iterator<T> dataIterator, int size, Offset nextOffset, boolean isFinish) {
        this.dataIterator = Objects.requireNonNull(dataIterator);
        this.size = size;
        this.nextOffset = Objects.requireNonNull(nextOffset);
        this.isFinish = isFinish;
    }

    public static <T> FetchData<T> createStreamFetch(List<T> dataList, Offset nextOffset, boolean isFinish) {
        return new FetchData<>(dataList.listIterator(), dataList.size(), nextOffset, isFinish);
    }

    public static <T> FetchData<T> createBatchFetch(Iterator<T> dataIterator, Offset nextOffset) {
        return new FetchData<>(dataIterator, -1, nextOffset, true);
    }

    /**
     * Returns data list.
     */
    public Iterator<T> getDataIterator() {
        return dataIterator;
    }

    /**
     * Returns data size.
     */
    public int getDataSize() {
        return size;
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
