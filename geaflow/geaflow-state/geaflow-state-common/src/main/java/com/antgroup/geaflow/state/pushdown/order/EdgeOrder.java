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

package com.antgroup.geaflow.state.pushdown.order;

import com.antgroup.geaflow.model.graph.meta.GraphFiledName;

public class EdgeOrder implements IEdgeOrder {

    private GraphFiledName filedName;
    private boolean desc;

    public EdgeOrder(GraphFiledName filedName, boolean desc) {
        this.filedName = filedName;
        this.desc = desc;
    }

    @Override
    public GraphFiledName getField() {
        return this.filedName;
    }

    @Override
    public boolean desc() {
        return this.desc;
    }
}
