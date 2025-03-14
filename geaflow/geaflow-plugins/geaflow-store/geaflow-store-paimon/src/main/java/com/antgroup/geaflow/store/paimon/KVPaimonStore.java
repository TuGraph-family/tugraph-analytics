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

package com.antgroup.geaflow.store.paimon;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.state.serializer.IKVSerializer;
import com.antgroup.geaflow.store.api.key.IKVStatefulStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.store.paimon.iterator.PaimonIterator;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Filter;

public class KVPaimonStore<K, V> extends BasePaimonStore implements IKVStatefulStore<K, V> {

    private static final String TABLE_NAME_PREFIX = "KVTable";

    private IKVSerializer<K, V> kvSerializer;

    private PaimonTableRWHandle tableHandle;

    private int[] projection;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.kvSerializer = (IKVSerializer<K, V>) Preconditions.checkNotNull(
            storeContext.getKeySerializer(), "keySerializer must be set");
        String tableName = TABLE_NAME_PREFIX + "#" + shardId;

        this.projection = new int[]{KEY_COLUMN_INDEX, VALUE_COLUMN_INDEX};
        Identifier identifier = new Identifier(paimonStoreName, tableName);
        this.tableHandle = createKVTableHandle(identifier);
    }

    @Override
    public void archive(long checkpointId) {
        this.lastCheckpointId = checkpointId;
        this.tableHandle.commit(lastCheckpointId);
    }

    @Override
    public void recovery(long checkpointId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long recoveryLatest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact() {

    }

    @Override
    public V get(K key) {
        byte[] binaryKey = this.kvSerializer.serializeKey(key);
        Filter<InternalRow> filter = row -> Arrays.equals(row.getBinary(KEY_COLUMN_INDEX),
            binaryKey);
        RecordReaderIterator<InternalRow> iterator = this.tableHandle.getIterator(filter,
            projection);
        try (PaimonIterator paimonIterator = new PaimonIterator(iterator)) {
            if (paimonIterator.hasNext()) {
                Tuple<byte[], byte[]> row = paimonIterator.next();
                return this.kvSerializer.deserializeValue(row.getF1());
            }
            return null;
        }
    }

    @Override
    public void put(K key, V value) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        byte[] valueArray = this.kvSerializer.serializeValue(value);
        GenericRow record = GenericRow.of(keyArray, valueArray);
        this.tableHandle.write(record, 0);
    }

    @Override
    public void remove(K key) {
        byte[] keyArray = this.kvSerializer.serializeKey(key);
        GenericRow record = GenericRow.ofKind(RowKind.DELETE, BinaryString.fromBytes(keyArray),
            null);
        this.tableHandle.write(record, 0);
    }

    @Override
    public void drop() {
        this.client.dropTable(tableHandle.getIdentifier());
        this.client.dropDatabase(paimonStoreName);
    }

    @Override
    public void flush() {
        this.tableHandle.flush(lastCheckpointId);
    }

    @Override
    public void close() {
        this.tableHandle.close();
        this.client.close();
    }
}
