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

/**
 * The dynamic graph state is the interface controlling
 * dynamic vertex/edge or one degree subgraph.
 * Dynamic graph is composed by multi graph snapshots,
 * and all the operators are made on the snapshots.
 */
public interface DynamicGraphState<K, VV, EV> {

    /**
     * Returns the dynamic vertex handler.
     */
    DynamicVertexState<K, VV, EV> V();

    /**
     * Returns the dynamic edge handler.
     */
    DynamicEdgeState<K, VV, EV> E();

    /**
     * Returns the one degree handler.
     */
    DynamicOneDegreeGraphState<K, VV, EV> VE();
}
