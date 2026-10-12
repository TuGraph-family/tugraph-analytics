/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api.model;

import java.util.ArrayList;
import java.util.List;

/** Stable minimum trace shape; future fields must be additive. */
public class RetrievalTrace {

    private String traceVersion;
    private String originalQuery;
    private RetrievalMode selectedMode;
    private ExecutionMode executionMode;
    private List<TraceStage> stages = new ArrayList<>();
    private String stopReason;

    public RetrievalTrace() {
    }

    public String getTraceVersion() {
        return traceVersion;
    }

    public void setTraceVersion(String traceVersion) {
        this.traceVersion = traceVersion;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public void setOriginalQuery(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public RetrievalMode getSelectedMode() {
        return selectedMode;
    }

    public void setSelectedMode(RetrievalMode selectedMode) {
        this.selectedMode = selectedMode;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(ExecutionMode executionMode) {
        this.executionMode = executionMode;
    }

    public List<TraceStage> getStages() {
        return stages;
    }

    public void setStages(List<TraceStage> stages) {
        this.stages = stages == null ? new ArrayList<TraceStage>() : new ArrayList<>(stages);
    }

    public String getStopReason() {
        return stopReason;
    }

    public void setStopReason(String stopReason) {
        this.stopReason = stopReason;
    }
}
