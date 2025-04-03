/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.common.iterator;

import java.util.Iterator;
import java.util.List;

public class ChainedCloseableIterator<T> implements CloseableIterator<T> {

    private final Iterator<CloseableIterator<T>> closeableIterators;
    private CloseableIterator<T> currentIterator;

    public ChainedCloseableIterator(List<CloseableIterator<T>> iterators) {
        if (iterators == null || iterators.isEmpty()) {
            this.currentIterator = null;
        }

        this.closeableIterators = iterators.iterator();
        this.currentIterator = closeableIterators.hasNext() ? closeableIterators.next() : null;
    }

    @Override
    public boolean hasNext() {
        while (currentIterator != null) {
            if (currentIterator.hasNext()) {
                return true;
            } else {
                currentIterator.close();
                currentIterator = closeableIterators.hasNext() ? closeableIterators.next() : null;
            }
        }

        return false;
    }

    @Override
    public T next() {
        return currentIterator.next();
    }

    @Override
    public void close() {
        if (currentIterator != null) {
            currentIterator.close();
        }

        while (closeableIterators.hasNext()) {
            CloseableIterator<T> next = closeableIterators.next();
            next.close();
        }
    }
}