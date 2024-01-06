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

package com.antgroup.geaflow.cluster.web.agent.model;

public class FlameGraphRequest {

    /**
     * Profiler type. Support CPU or ALLOC.
     */
    private FlameGraphType type;

    /**
     * Duration of async-profiler execution, in second time-unit.
     */
    private int duration;

    /**
     * The pid to profile.
     */
    private int pid;

    public FlameGraphType getType() {
        return type;
    }

    public void setType(FlameGraphType type) {
        this.type = type;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }
}
