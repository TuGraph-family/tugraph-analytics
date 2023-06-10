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

package com.antgroup.geaflow.store.memory;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.StoreDesc;
import java.util.Arrays;
import java.util.List;

public class MemoryStoreBuilder implements IStoreBuilder {

    private static final StoreDesc STORE_DESC = new MemoryStoreDesc();

    public IBaseStore getStore(DataModel type, Configuration config) {
        switch (type) {
            case DYNAMIC_GRAPH:
                return new GraphMemoryMultiVersionedStore<>();
            case STATIC_GRAPH:
                boolean csrEnable = config.getBoolean(MemoryConfigKeys.CSR_MEMORY_ENABLE);
                return csrEnable ? new GraphMemoryCSRStore<>() : new GraphMemoryStore<>();
            case KV:
                return new KVMemoryStore<>();
            case KList:
                return new KListMemoryStore<>();
            case KMap:
                return new KMapMemoryStore<>();
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError("not support " + type));
        }
    }

    @Override
    public StoreDesc getStoreDesc() {
        return STORE_DESC;
    }

    @Override
    public List<DataModel> supportedDataModel() {
        return Arrays.asList(DataModel.values());
    }

    public static class MemoryStoreDesc implements StoreDesc {

        @Override
        public boolean isLocalStore() {
            return false;
        }

        @Override
        public String name() {
            return StoreType.MEMORY.name();
        }
    }
}
