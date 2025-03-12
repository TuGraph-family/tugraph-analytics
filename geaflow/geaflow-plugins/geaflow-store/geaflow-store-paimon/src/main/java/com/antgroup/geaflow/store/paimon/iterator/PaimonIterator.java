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

package com.antgroup.geaflow.store.paimon.iterator;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.tuple.Tuple;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;

public class PaimonIterator implements CloseableIterator<Tuple<byte[], byte[]>> {

    private final RecordReaderIterator<InternalRow> paimonRowIter;
    private Tuple<byte[], byte[]> next;
    private boolean isClosed = false;

    public PaimonIterator(RecordReaderIterator<InternalRow> iterator) {
        this.paimonRowIter = iterator;
    }

    @Override
    public boolean hasNext() {
        next = null;
        if (!isClosed && this.paimonRowIter.hasNext()) {
            InternalRow nextRow = this.paimonRowIter.next();
            next = Tuple.of(nextRow.getBinary(0), nextRow.getBinary(1));
        }
        if (next == null) {
            close();
            return false;
        }
        return true;
    }

    @Override
    public Tuple<byte[], byte[]> next() {
        return next;
    }

    @Override
    public void close() {
        if (!isClosed) {
            try {
                this.paimonRowIter.close();
            } catch (Exception e) {
                throw new GeaflowRuntimeException("Close paimon iterator failed.", e);
            }
            isClosed = true;
        }
    }
}