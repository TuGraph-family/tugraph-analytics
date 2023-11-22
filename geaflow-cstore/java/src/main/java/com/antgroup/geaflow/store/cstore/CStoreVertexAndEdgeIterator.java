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
package com.antgroup.geaflow.store.cstore;

public class CStoreVertexAndEdgeIterator extends NativeObject implements CloseableIterator<VertexAndEdgeContainer> {

    private VertexAndEdgeContainer nextVal;

    public CStoreVertexAndEdgeIterator(long iterHandler) {
        super(iterHandler);
    }

    @Override
    public boolean hasNext() {
        nextVal = next(this.nativeHandle);
        return nextVal != null;
    }

    @Override
    public VertexAndEdgeContainer next() {
        return nextVal;
    }

    static native VertexAndEdgeContainer next(long handler_);

    protected native void disposeInternal(long handle_);
}
