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

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * This class accepts a iterator and makes its value filtered.
 */
public class IteratorWithFilter<T> extends BaseCloseableIterator<T, T> {

    private final Predicate<T> predicate;
    private T nextValue;

    public IteratorWithFilter(CloseableIterator<T> iterator, Predicate<T> predicate) {
        super(iterator);
        this.predicate = Preconditions.checkNotNull(predicate);
    }

    public IteratorWithFilter(Iterator<T> iterator, Predicate<T> predicate) {
        super(iterator);
        this.predicate = Preconditions.checkNotNull(predicate);
    }
    
    @Override
    public boolean hasNext() {
        while (this.iterator.hasNext()) {
            nextValue = this.iterator.next();
            if (!predicate.test(nextValue)) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public T next() {
        return nextValue;
    }
}
