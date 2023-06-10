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

package com.antgroup.geaflow.store.rocksdb.iterator;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.utils.ByteUtils;
import java.io.Closeable;
import java.util.Iterator;
import org.rocksdb.RocksIterator;

public class RocksdbIterator implements Iterator<Tuple<byte[], byte[]>>, Closeable {

    private final RocksIterator rocksIt;
    private byte[] prefix;
    private Tuple<byte[], byte[]> next;
    private boolean isClosed = false;

    public RocksdbIterator(RocksIterator iterator) {
        this.rocksIt = iterator;
        this.rocksIt.seekToFirst();
    }

    public RocksdbIterator(RocksIterator iterator, byte[] prefix) {
        this.rocksIt = iterator;
        this.prefix = prefix;
        this.rocksIt.seek(prefix);
    }

    private boolean isValid(byte[] key) {
        return prefix == null || ByteUtils.isStartsWith(key, prefix);
    }

    @Override
    public boolean hasNext() {
        next = null;
        if (!isClosed && this.rocksIt.isValid()) {
            next = Tuple.of(this.rocksIt.key(), this.rocksIt.value());
        }
        if (next == null || !isValid(next.f0)) {
            close();
            return false;
        }
        return true;
    }

    @Override
    public Tuple<byte[], byte[]> next() {
        this.rocksIt.next();
        return next;
    }

    @Override
    public void close() {
        if (!isClosed) {
            this.rocksIt.close();
            isClosed = true;
        }
    }
}
