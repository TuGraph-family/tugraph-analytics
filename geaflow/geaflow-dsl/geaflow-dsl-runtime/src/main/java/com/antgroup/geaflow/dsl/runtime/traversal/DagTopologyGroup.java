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

package com.antgroup.geaflow.dsl.runtime.traversal;

import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepOperator;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DagTopologyGroup {

    private final DagTopology mainDag;

    private final Map<String, DagTopology> subDags;

    private final Map<Long, StepOperator<StepRecord, StepRecord>> globalOpId2Operators;

    public DagTopologyGroup(DagTopology mainDag,
                            Map<String, DagTopology> subDags) {
        this.mainDag = mainDag;
        this.subDags = subDags;
        this.globalOpId2Operators = new HashMap<>();
        this.globalOpId2Operators.putAll(mainDag.getOpId2Operators());

        for (DagTopology subDag : subDags.values()) {
            Map<Long, StepOperator<StepRecord, StepRecord>> id2Operators = subDag.getOpId2Operators();
            for (Map.Entry<Long, StepOperator<StepRecord, StepRecord>> entry : id2Operators.entrySet()) {
                long opId = entry.getKey();
                if (globalOpId2Operators.containsKey(opId)) {
                    throw new GeaFlowDSLException("Operator id: " + opId + " in sub dag: " + subDag.getQueryName()
                        + " is conflict with other dag.");
                }
                globalOpId2Operators.put(opId, entry.getValue());
            }
        }
    }

    public List<Long> getOutputIds(long opId) {
        if (mainDag.contains(opId)) {
            return mainDag.getOutputIds(opId);
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId)) {
                return subDag.getOutputIds(opId);
            }
        }
        throw new IllegalArgumentException("Illegal opId: " + opId);
    }

    public List<Long> getInputIds(long opId) {
        if (mainDag.contains(opId)) {
            return mainDag.getInputIds(opId);
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId)) {
                return subDag.getInputIds(opId);
            }
        }
        throw new IllegalArgumentException("Illegal opId: " + opId);
    }

    public DagTopology getDagTopology(long opId) {
        if (mainDag.contains(opId)) {
            return mainDag;
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId)) {
                return subDag;
            }
        }
        throw new IllegalArgumentException("Illegal opId: " + opId);
    }

    public boolean isChained(long opId1, long opId2) {
        if (mainDag.contains(opId1) && mainDag.contains(opId2)) {
            return mainDag.isChained(opId1, opId2);
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId1) && subDag.contains(opId2)) {
                return subDag.isChained(opId1, opId2);
            }
        }
        return false;
    }

    public boolean belongMainDag(long opId) {
        return mainDag.contains(opId);
    }

    @SuppressWarnings("unchecked")
    public StepOperator getOperator(long opId) {
        return globalOpId2Operators.get(opId);
    }

    public DagTopology getMainDag() {
        return mainDag;
    }

    public List<DagTopology> getAllDagTopology() {
        List<DagTopology> dagTopologies = new ArrayList<>();
        dagTopologies.add(mainDag);
        dagTopologies.addAll(subDags.values());
        return dagTopologies;
    }

    public List<DagTopology> getSubDagTopologies() {
        return Lists.newArrayList(subDags.values());
    }

    public Collection<StepOperator<StepRecord, StepRecord>> getAllOperators() {
        return globalOpId2Operators.values();
    }
}
