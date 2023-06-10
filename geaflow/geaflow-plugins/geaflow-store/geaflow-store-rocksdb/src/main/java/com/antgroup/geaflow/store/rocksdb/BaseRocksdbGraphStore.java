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

package com.antgroup.geaflow.store.rocksdb;

import com.antgroup.geaflow.state.pushdown.inner.CodeGenFilterConverter;
import com.antgroup.geaflow.state.pushdown.inner.DirectFilterConverter;
import com.antgroup.geaflow.state.pushdown.inner.IFilterConverter;
import com.antgroup.geaflow.store.api.graph.IPushDownStore;
import com.antgroup.geaflow.store.config.StoreConfigKeys;
import com.antgroup.geaflow.store.context.StoreContext;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class BaseRocksdbGraphStore extends BaseRocksdbStore implements IPushDownStore {

    protected IFilterConverter filterConverter;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        boolean codegenEnable =
            storeContext.getConfig().getBoolean(StoreConfigKeys.STORE_FILTER_CODEGEN_ENABLE);
        filterConverter = codegenEnable ? new CodeGenFilterConverter() : new DirectFilterConverter();
    }

    @Override
    public IFilterConverter getFilterConverter() {
        return filterConverter;
    }

    @Override
    protected Path getRemotePath() {
        return Paths.get(root, storeContext.getName(),
            Integer.toString(shardId));
    }
}
