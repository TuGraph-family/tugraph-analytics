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

package com.antgroup.geaflow.store;

import com.antgroup.geaflow.state.pushdown.inner.CodeGenFilterConverter;
import com.antgroup.geaflow.state.pushdown.inner.DirectFilterConverter;
import com.antgroup.geaflow.state.pushdown.inner.IFilterConverter;
import com.antgroup.geaflow.store.api.graph.IPushDownStore;
import com.antgroup.geaflow.store.config.StoreConfigKeys;
import com.antgroup.geaflow.store.context.StoreContext;

public abstract class BaseGraphStore extends AbstractBaseStore implements IPushDownStore {

    protected StoreContext storeContext;
    protected IFilterConverter filterConverter;

    @Override
    public void init(StoreContext storeContext) {
        this.storeContext = storeContext;
        boolean codegenEnable =
            storeContext.getConfig().getBoolean(StoreConfigKeys.STORE_FILTER_CODEGEN_ENABLE);
        filterConverter = codegenEnable ? new CodeGenFilterConverter() : new DirectFilterConverter();
    }

    @Override
    public IFilterConverter getFilterConverter() {
        return filterConverter;
    }

    @Override
    public void archive(long checkpointId) {

    }

    @Override
    public void recovery(long checkpointId) {

    }

    @Override
    public long recoveryLatest() {
        return 0;
    }

    @Override
    public void compact() {

    }

    @Override
    public void flush() {

    }

}
