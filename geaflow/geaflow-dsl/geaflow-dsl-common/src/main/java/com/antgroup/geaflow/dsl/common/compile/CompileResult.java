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

package com.antgroup.geaflow.dsl.common.compile;

import com.antgroup.geaflow.common.visualization.console.JsonPlan;
import java.io.Serializable;
import java.util.Set;

public class CompileResult implements Serializable {

    private JsonPlan physicPlan;

    private Set<TableInfo> sourceTables;

    private Set<TableInfo> targetTables;

    private Set<GraphInfo> sourceGraphs;

    private Set<GraphInfo> targetGraphs;

    public CompileResult() {

    }

    public JsonPlan getPhysicPlan() {
        return physicPlan;
    }

    public void setPhysicPlan(JsonPlan physicPlan) {
        this.physicPlan = physicPlan;
    }

    public Set<TableInfo> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(Set<TableInfo> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public Set<TableInfo> getTargetTables() {
        return targetTables;
    }

    public void setTargetTables(Set<TableInfo> targetTables) {
        this.targetTables = targetTables;
    }

    public Set<GraphInfo> getSourceGraphs() {
        return sourceGraphs;
    }

    public void setSourceGraphs(Set<GraphInfo> sourceGraphs) {
        this.sourceGraphs = sourceGraphs;
    }

    public Set<GraphInfo> getTargetGraphs() {
        return targetGraphs;
    }

    public void setTargetGraphs(Set<GraphInfo> targetGraphs) {
        this.targetGraphs = targetGraphs;
    }
}
