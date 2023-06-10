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

import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.manage.ManageableGraphState;
import com.antgroup.geaflow.state.manage.ManageableGraphStateImpl;
import com.antgroup.geaflow.state.strategy.manager.GraphManagerImpl;
import com.antgroup.geaflow.state.strategy.manager.IGraphManager;

public class GraphStateImpl<K, VV, EV> implements GraphState<K, VV, EV> {

    private final IGraphManager<K, VV, EV> graphManager;
    private final ManageableGraphState manageableGraphState;

    private StaticGraphState<K, VV, EV> staticGraphState;
    private DynamicGraphState<K, VV, EV> dynamicGraphState;

    public GraphStateImpl(StateContext context) {
        this.graphManager = new GraphManagerImpl<>();
        this.graphManager.init(context);
        this.manageableGraphState = new ManageableGraphStateImpl(this.graphManager);
        this.staticGraphState = new StaticGraphStateImpl<>(this.graphManager);
        this.dynamicGraphState = new DynamicGraphStateImpl<>(this.graphManager);
    }

    @Override
    public StaticGraphState<K, VV, EV> staticGraph() {
        return staticGraphState;
    }

    @Override
    public DynamicGraphState<K, VV, EV> dynamicGraph() {
        return dynamicGraphState;
    }

    @Override
    public ManageableGraphState manage() {
        return manageableGraphState;
    }

}
