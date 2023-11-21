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
import java.util.Iterator;
import java.util.function.Function;

/**
 * This class accepts a iterator and makes its value transformed.
 */
public class IteratorWithFn<T, R> extends BaseCloseableIterator<T, R> {

    protected final Function<T, R> fn;

    public IteratorWithFn(CloseableIterator<T> iterator, Function<T, R> function) {
        super(iterator);
        this.fn = function;
    }

    public IteratorWithFn(Iterator<T> iterator, Function<T, R> function) {
        super(iterator);
        this.fn = function;
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public R next() {
        return fn.apply(iterator.next());
    }
}
