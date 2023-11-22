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

public class CStoreEdgeIterator extends NativeObject implements CloseableIterator<EdgeContainer> {

    private EdgeContainer nextVal;

    public CStoreEdgeIterator(long edgeIterHandler) {
        super(edgeIterHandler);
    }

    @Override
    public boolean hasNext() {
        byte[] nextBytes = next(this.nativeHandle);
        if (nextBytes == null || nextBytes.length == 0) {
            return false;
        }
        nextVal = new EdgeContainer(nextBytes);
        return true;
    }

    @Override
    public EdgeContainer next() {
        return nextVal;
    }

    static native byte[] next(long handler_);

    protected native void disposeInternal(long handle);
}
