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

import java.util.Map;

public class NativeGraphStore extends NativeObject {

    protected NativeGraphStore(String name, int shard, Map<String, String> config) {
        super(construct(name, shard, config));
    }

    static native long construct(String name, int shard, Map<String, String> config);

    /**
     * Store Actions: flush/close/compact/archive/recover.
     */

    private static native void flush(long handler_);
    private static native void close(long handler_);
    private static native void compact(long handler_);
    private static native void archive(long handler_, long version);
    private static native void recover(long handler_, long version);

    public void flush() {
        flush(super.nativeHandle);
    }

    public void close() {
        if (this.owningHandle_.compareAndSet(true, false)) {
            close(super.nativeHandle);
            disposeInternal(nativeHandle);
        }
    }

    public void compact() {
        compact(super.nativeHandle);
    }

    public void archive(long version) {
        archive(super.nativeHandle, version);
    }

    public void recover(long version) {
        recover(super.nativeHandle, version);
    }

    /**
     * Vertex Operators.
     */
    private static native void addVertex(long handler_, byte[] key, long ts, String label, byte[] value);
    private native long getVertex(long handler_, byte[] key);
    private native long getVertexWithPushdown(long handler_, byte[] key, byte[] pushdown);
    private native long scanVertex(long handler_);
    private native long scanVertexWithPushdown(long handler_, byte[] pushdown);
    private native long scanVertexWithKeys(long handler_, byte[][] keys, int keyNum);
    private native long scanVertexWithKeysAndPushdown(long handler_, byte[][] keys, int keyNum, byte[] pushdown);

    public void addVertex(VertexContainer container) {
        addVertex(super.nativeHandle, container.id, container.ts, container.label, container.property);
    }

    public CStoreVertexIterator getVertex(byte[] key) {
        long handler = getVertex(super.nativeHandle, key);
        return new CStoreVertexIterator(handler);
    }

    public CStoreVertexIterator getVertex(byte[] key, byte[] pushdown) {
        long handler = getVertexWithPushdown(super.nativeHandle, key, pushdown);
        return new CStoreVertexIterator(handler);
    }

    public CStoreVertexIterator scanVertex() {
        long handler = scanVertex(super.nativeHandle);
        return new CStoreVertexIterator(handler);
    }

    public CStoreVertexIterator scanVertex(byte[] pushdown) {
        long handler = scanVertexWithPushdown(super.nativeHandle, pushdown);
        return new CStoreVertexIterator(handler);
    }

    public CStoreVertexIterator scanVertex(byte[][] keys) {
        long handler = scanVertexWithKeys(super.nativeHandle, keys, keys.length);
        return new CStoreVertexIterator(handler);
    }

    public CStoreVertexIterator scanVertex(byte[][] keys, byte[] pushdown) {
        long handler = scanVertexWithKeysAndPushdown(super.nativeHandle, keys, keys.length, pushdown);
        return new CStoreVertexIterator(handler);
    }

    /**
     * Edge Operators.
     */
    private static native void addEdge(long handler_, byte[] sid, long ts, String label,
                                       boolean isOut, byte[] tid, byte[] value);
    private native long getEdges(long handler_, byte[] key);
    private native long getEdgesWithPushdown(long handler_, byte[] key, byte[] pushdown);
    private native long scanEdges(long handler_);
    private native long scanEdgesWithPushdown(long handler_, byte[] pushdown);
    private native long scanEdgesWithKeys(long handler_, byte[][] keys, int keyNum);
    private native long scanEdgesWithKeysAndPushdown(long handler_, byte[][] keys, int keyNum,
                                                     byte[] pushdown);

    public void addEdge(EdgeContainer container) {
        addEdge(super.nativeHandle, container.sid, container.ts, container.label, container.isOut,
            container.tid, container.property);
    }

    public CStoreEdgeIterator getEdges(byte[] key) {
        long iteratorHandler = getEdges(super.nativeHandle, key);
        return new CStoreEdgeIterator(iteratorHandler);
    }

    public CStoreEdgeIterator getEdges(byte[] key, byte[] pushdown) {
        long iteratorHandler = getEdgesWithPushdown(super.nativeHandle, key, pushdown);
        return new CStoreEdgeIterator(iteratorHandler);
    }

    public CStoreEdgeIterator scanEdges() {
        long handler = scanEdges(super.nativeHandle);
        return new CStoreEdgeIterator(handler);
    }

    public CStoreEdgeIterator scanEdges(byte[] pushdown) {
        long handler = scanEdgesWithPushdown(super.nativeHandle, pushdown);
        return new CStoreEdgeIterator(handler);
    }

    public CStoreEdgeIterator scanEdges(byte[][] keys) {
        long handler = scanEdgesWithKeys(super.nativeHandle, keys, keys.length);
        return new CStoreEdgeIterator(handler);
    }

    public CStoreEdgeIterator scanEdges(byte[][] keys, byte[] pushdown) {
        long handler = scanEdgesWithKeysAndPushdown(super.nativeHandle, keys, keys.length,
            pushdown);
        return new CStoreEdgeIterator(handler);
    }

    /**
     * VertexAndEdge Operators.
     */

    private native long[] getVertexAndEdge(long handler_, byte[] key);
    private native long[] getVertexAndEdgeWithPushdown(long handler_, byte[] key, byte[] pushdown);
    private native long scanVertexAndEdge(long handler_);
    private native long scanVertexAndEdgeWithPushdown(long handler_, byte[] pushdown);
    private native long scanVertexAndEdgeWithKeys(long handler_, byte[][] keys, int keyNum);
    private native long scanVertexAndEdgeWithKeysAndPushdown(long handler_, byte[][] keys,
                                                             int keyNum, byte[] pushdown);

    public VertexAndEdge getVertexAndEdge(byte[] key) {
        long[] addr = getVertexAndEdge(super.nativeHandle, key);
        return new VertexAndEdge(key, addr[0], addr[1]);
    }

    public VertexAndEdge getVertexAndEdge(byte[] key, byte[] pushdown) {
        long[] addr = getVertexAndEdgeWithPushdown(super.nativeHandle, key, pushdown);
        return new VertexAndEdge(key, addr[0], addr[1]);
    }

    public CStoreVertexAndEdgeIterator scanVertexAndEdge() {
        long handler = scanVertexAndEdge(super.nativeHandle);
        return new CStoreVertexAndEdgeIterator(handler);
    }

    public CStoreVertexAndEdgeIterator scanVertexAndEdge(byte[] pushdown) {
        long handler = scanVertexAndEdgeWithPushdown(super.nativeHandle, pushdown);
        return new CStoreVertexAndEdgeIterator(handler);
    }

    public CStoreVertexAndEdgeIterator scanVertexAndEdge(byte[][] keys) {
        long iteratorHandler = scanVertexAndEdgeWithKeys(super.nativeHandle, keys, keys.length);
        return new CStoreVertexAndEdgeIterator(iteratorHandler);
    }

    public CStoreVertexAndEdgeIterator scanVertexAndEdge(byte[][] keys, byte[] pushdown) {
        long iteratorHandler = scanVertexAndEdgeWithKeysAndPushdown(super.nativeHandle, keys,
            keys.length, pushdown);
        return new CStoreVertexAndEdgeIterator(iteratorHandler);
    }

    @Override
    protected native void disposeInternal(long handle);

}
