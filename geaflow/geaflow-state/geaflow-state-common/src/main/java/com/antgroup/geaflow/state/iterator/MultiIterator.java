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
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class is an adaptation of Guava's Iterators.concat
 * by fixing the issue https://github.com/google/guava/issues/3178.
 */
public class MultiIterator<T> implements CloseableIterator<T> {

    private final Iterator<? extends CloseableIterator<? extends T>> iterators;
    private CloseableIterator<? extends T> currIterator;
    private T nextValue;

    public MultiIterator(Iterator<? extends CloseableIterator<? extends T>> iterators) {
        this.iterators = iterators;
        if (iterators.hasNext()) {
            this.currIterator = iterators.next();
        }
    }

    public MultiIterator(CloseableIterator<? extends T>... iteratorCandidates) {
        this.iterators = Arrays.asList(iteratorCandidates).iterator();
        if (iterators.hasNext()) {
            this.currIterator = iterators.next();
        }
    }

    @Override
    public boolean hasNext() {
        if (currIterator == null) {
            return false;
        }
        if (!currIterator.hasNext()) {
            currIterator.close();
            do {
                if (!this.iterators.hasNext()) {
                    return false;
                }

                currIterator = this.iterators.next();
            } while (!currIterator.hasNext());
        }
        nextValue = currIterator.next();
        return true;
    }

    @Override
    public T next() {
        return nextValue;
    }

    @Override
    public void close() {
        if (currIterator != null) {
            currIterator.close();
        }
        while (this.iterators.hasNext()) {
            currIterator = this.iterators.next();
            currIterator.close();
        }
    }
}
