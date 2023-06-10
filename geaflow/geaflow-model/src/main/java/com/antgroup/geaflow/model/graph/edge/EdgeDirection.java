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

package com.antgroup.geaflow.model.graph.edge;

public enum EdgeDirection {
    /**
     * The in edge.
     */
    IN,
    /**
     * The out edge.
     */
    OUT,
    /**
     * Include In and Out Edges.
     */
    BOTH,

    /**
     * None direction edge.
     */
    NONE
    ;

    public EdgeDirection reverse() {
        switch (this) {
            case IN:
                return OUT;
            case OUT:
                return IN;
            default:
                return this;
        }
    }
}
