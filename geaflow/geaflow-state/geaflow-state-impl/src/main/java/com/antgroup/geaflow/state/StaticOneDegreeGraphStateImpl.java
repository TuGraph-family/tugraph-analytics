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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.state.data.DataType;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.query.QueryType;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;
import java.util.Collection;

public class StaticOneDegreeGraphStateImpl<K, VV, EV> extends BaseQueryState<K, VV, EV, OneDegreeGraph<K, VV, EV>>
    implements StaticOneDegreeGraphState<K, VV, EV>,
    RevisableState<K, OneDegreeGraph<K, VV, EV>> {

    public StaticOneDegreeGraphStateImpl(IGraphManager<K, VV, EV> manager) {
        super(new QueryType<>(DataType.VE), manager);
    }

    @Override
    public void add(OneDegreeGraph<K, VV, EV> oneDegreeGraph) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void update(OneDegreeGraph<K, VV, EV> oneDegreeGraph) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(OneDegreeGraph<K, VV, EV> oneDegreeGraph) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(K... ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(Collection<K> ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
