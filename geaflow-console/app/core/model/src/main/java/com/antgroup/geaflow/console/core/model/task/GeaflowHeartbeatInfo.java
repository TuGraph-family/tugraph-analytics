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

package com.antgroup.geaflow.console.core.model.task;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowHeartbeatInfo {

    private Integer activeNum;

    private Integer totalNum;

    private Long expiredTimeMs;

    private List<ContainerInfo> containers;

    @Getter
    @Setter
    public static class ContainerInfo {

        private Integer id;

        private String name;

        private String host;

        private int pid;

        private Long lastTimestamp;

        private boolean active;

        private ProcessMetric metrics;

        @Getter
        @Setter
        public static class ProcessMetric {

            // This amount of memory is guaranteed for the Java virtual machine to use.
            private long heapCommittedMB;
            private long heapUsedMB;
            private double heapUsedRatio;
            private long totalMemoryMB;

            // the total number of full collections that have occurred.
            private long fgcCount = 0L;

            // the total cost of full collections that have occurred.
            private long fgcTime = 0L;

            // the approximate accumulated collection elapsed time in milliseconds.
            private long gcTime = 0L;

            // the total number of collections that have occurred.
            private long gcCount = 0;

            // The system load average for the last minute, or a negative value if not available.
            private double avgLoad;

            // the number of processors available to the Java virtual machine.
            private int availCores;

            // cpu usage.
            private double processCpu;

            // the number of processors used.
            private double usedCores;
            private int activeThreads;
        }
    }

}
