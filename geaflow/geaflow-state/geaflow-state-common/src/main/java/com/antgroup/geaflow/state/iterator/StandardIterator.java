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

import java.util.Iterator;

/**
 * This class is a wrapper iterator, allowing multiple hasNext call but one next call.
 */
public class StandardIterator<T> implements Iterator<T> {

    private final Iterator<T> iterator;
    private boolean nextCalled;
    private boolean hasNextValue;
    private T nextValue;

    public StandardIterator(Iterator<T> iterator) {
        this.iterator = iterator;
        innerNext();
    }

    private void innerNext() {
        this.hasNextValue = this.iterator.hasNext();
        this.nextValue = this.hasNextValue ? this.iterator.next() : null;
        this.nextCalled = false;
    }

    @Override
    public boolean hasNext() {
        // only next has called, we trigger hasNext method.
        if (nextCalled) {
            innerNext();
        }
        return hasNextValue;
    }

    @Override
    public T next() {
        if (nextValue != null) {
            nextCalled = true;
            T next = nextValue;
            nextValue = null;
            return next;
        }
        return null;
        // throw new NoSuchElementException("hasNext not called or has no next data");
    }
}
