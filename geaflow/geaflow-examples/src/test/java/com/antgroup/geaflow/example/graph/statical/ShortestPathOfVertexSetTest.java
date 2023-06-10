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

package com.antgroup.geaflow.example.graph.statical;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;
import static com.antgroup.geaflow.cluster.constants.ClusterConstants.LOCAL_CLUSTER;

import com.antgroup.geaflow.example.graph.statical.compute.shortestpathofvertexsets.ShortestPathOfVertexSet;
import org.testng.annotations.Test;

public class ShortestPathOfVertexSetTest {

    @Test
    public void testMainInvoke() {
        System.setProperty(CLUSTER_TYPE, LOCAL_CLUSTER);
        ShortestPathOfVertexSet.main(null);
    }

}
