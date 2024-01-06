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

public abstract class BaseCloseableIterator<I, O> implements CloseableIterator<O> {

    protected final Iterator<I> iterator;
    protected final boolean closeable;

    public BaseCloseableIterator(CloseableIterator<I> iterator) {
        this.iterator = iterator;
        this.closeable = true;
    }

    public BaseCloseableIterator(Iterator<I> iterator) {
        this.iterator = iterator;
        this.closeable = iterator instanceof AutoCloseable;
    }

    @Override
    public void close() {
        if (this.closeable) {
            try {
                ((AutoCloseable)this.iterator).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
