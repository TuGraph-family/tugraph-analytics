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

package com.antgroup.geaflow.store.paimon;

import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.ILocalStore;
import com.antgroup.geaflow.store.context.StoreContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

public abstract class BasePaimonStore implements IBaseStore, ILocalStore {

    protected static final String KEY_COLUMN_NAME = "key";

    protected static final String VALUE_COLUMN_NAME = "value";

    protected static final int KEY_COLUMN_INDEX = 0;

    protected static final int VALUE_COLUMN_INDEX = 1;

    protected PaimonTableCatalogClient client;

    protected int shardId;

    protected String jobName;

    protected String paimonStoreName;

    protected long lastCheckpointId;

    @Override
    public void init(StoreContext storeContext) {
        this.shardId = storeContext.getShardId();
        this.jobName = storeContext.getConfig().getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.paimonStoreName = this.jobName + "#" + this.shardId;
        this.client = new PaimonTableCatalogClient(storeContext.getConfig());
        this.lastCheckpointId = Long.MAX_VALUE;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public void initShardId(int shardId) {
        this.shardId = shardId;
    }

    protected PaimonTableRWHandle createKVTableHandle(Identifier identifier) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey(KEY_COLUMN_NAME);
        schemaBuilder.column(KEY_COLUMN_NAME, DataTypes.BYTES());
        schemaBuilder.column(VALUE_COLUMN_NAME, DataTypes.BYTES());
        Schema schema = schemaBuilder.build();
        Table vertexTable = this.client.createTable(schema, identifier);
        return new PaimonTableRWHandle(identifier, vertexTable);
    }
}
