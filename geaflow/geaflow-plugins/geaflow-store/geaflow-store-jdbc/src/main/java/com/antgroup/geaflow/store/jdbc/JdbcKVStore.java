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

package com.antgroup.geaflow.store.jdbc;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.RetryCommand;
import com.antgroup.geaflow.state.serializer.IKVSerializer;
import com.antgroup.geaflow.store.api.key.IKVStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.google.common.base.Preconditions;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcKVStore<K, V> extends BasicJdbcStore implements IKVStore<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcKVStore.class);
    private String[] columns = {"value"};

    private IKVSerializer<K, V> serializer;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.serializer = (IKVSerializer<K, V>) Preconditions.checkNotNull(storeContext.getKeySerializer(),
            "keySerializer must be set");
    }

    private java.lang.String getFromKey(K key) {
        if (key.getClass() == byte[].class) {
            return new String((byte[])key);
        }
        return key.toString();
    }

    @Override
    public void drop() {

    }

    @Override
    public V get(K key) {
        try {
            byte[] res = query(getFromKey(key), columns);
            if (res != null) {
                return serializer.deserializeValue(res);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new GeaflowRuntimeException("get fail", e);
        }
    }

    @Override
    public void put(K key, V value) {
        byte[] valueArray = serializer.serializeValue(value);
        RetryCommand.run(() -> {
            try {
                if (!update(getFromKey(key), columns, new Object[] {valueArray})
                    && !insert(getFromKey(key), columns, new Object[] {valueArray})) {
                    throw new GeaflowRuntimeException("put fail");
                }
            } catch (SQLException e) {
                throw new GeaflowRuntimeException("put fail", e);
            }
            return true;
        }, retries);
    }

    @Override
    public void remove(K key) {
        try {
            delete(getFromKey(key));
        } catch (SQLException e) {
            throw new GeaflowRuntimeException("remove fail", e);
        }
    }
}
