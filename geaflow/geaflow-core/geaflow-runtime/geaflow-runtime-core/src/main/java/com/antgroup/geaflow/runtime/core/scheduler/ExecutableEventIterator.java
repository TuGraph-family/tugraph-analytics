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

package com.antgroup.geaflow.runtime.core.scheduler;

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.protocol.FinishPrefetchEvent;
import com.antgroup.geaflow.runtime.core.protocol.PrefetchEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ExecutableEventIterator {

    private final Map<WorkerInfo, List<ExecutableEvent>> worker2events = new TreeMap<>();
    private int size = 0;
    private Iterator<Map.Entry<WorkerInfo, List<ExecutableEvent>>> workerIterator;
    private boolean ready = false;

    public Map<WorkerInfo, List<ExecutableEvent>> getEvents() {
        return this.worker2events;
    }

    public void markReady() {
        for (Map.Entry<WorkerInfo, List<ExecutableEvent>> entry : this.worker2events.entrySet()) {
            List<ExecutableEvent> events = entry.getValue();
            List<ExecutableEvent> finishPrefetchEvents = new ArrayList<>();
            for (ExecutableEvent executableEvent : events) {
                IEvent event = executableEvent.getEvent();
                if (event.getEventType() == EventType.PREFETCH) {
                    PrefetchEvent prefetchEvent = (PrefetchEvent) event;
                    FinishPrefetchEvent finishPrefetchEvent = new FinishPrefetchEvent(
                        prefetchEvent.getSchedulerId(),
                        prefetchEvent.getWorkerId(),
                        prefetchEvent.getCycleId(),
                        prefetchEvent.getIterationWindowId(),
                        executableEvent.getTask().getTaskId(),
                        executableEvent.getTask().getIndex(),
                        prefetchEvent.getPipelineId(),
                        prefetchEvent.getEdgeIds());
                    ExecutableEvent finishExecutableEvent = ExecutableEvent.build(
                        executableEvent.getWorker(), executableEvent.getTask(), finishPrefetchEvent);
                    finishPrefetchEvents.add(finishExecutableEvent);
                }
            }
            events.addAll(finishPrefetchEvents);
        }
        this.workerIterator = this.worker2events.entrySet().iterator();
        this.ready = true;
    }

    public ExecutableEventIterator merge(ExecutableEventIterator other) {
        for (List<ExecutableEvent> events : other.getEvents().values()) {
            for (ExecutableEvent event : events) {
                this.addEvent(event);
            }
        }
        return this;
    }

    public int size() {
        return this.size;
    }

    //////////////////////////////
    // Produce event.
    //////////////////////////////

    public void addEvent(WorkerInfo worker, ExecutionTask task, IEvent event) {
        this.addEvent(ExecutableEvent.build(worker, task, event));
    }

    public void addEvent(ExecutableEvent event) {
        if (this.ready) {
            throw new GeaflowRuntimeException("event iterator already mark ready");
        }
        List<ExecutableEvent> events = this.worker2events.computeIfAbsent(event.getWorker(), w -> new ArrayList<>());
        events.add(event);
        this.size++;
    }


    //////////////////////////////
    // Consume event.
    //////////////////////////////

    public boolean hasNext() {
        if (!this.ready) {
            throw new GeaflowRuntimeException("event iterator not ready");
        }
        return this.workerIterator.hasNext();
    }

    public Tuple<WorkerInfo, List<ExecutableEvent>> next() {
        Map.Entry<WorkerInfo, List<ExecutableEvent>> next = this.workerIterator.next();
        return Tuple.of(next.getKey(), next.getValue());
    }

    public static class ExecutableEvent {

        private final WorkerInfo worker;
        private final ExecutionTask task;
        private final IEvent event;

        private ExecutableEvent(WorkerInfo worker, ExecutionTask task, IEvent event) {
            this.worker = worker;
            this.task = task;
            this.event = event;
        }

        public WorkerInfo getWorker() {
            return this.worker;
        }

        public ExecutionTask getTask() {
            return this.task;
        }

        public IEvent getEvent() {
            return this.event;
        }

        @Override
        public String toString() {
            return String.valueOf(this.event);
        }

        public static ExecutableEvent build(WorkerInfo worker, ExecutionTask task, IEvent event) {
            return new ExecutableEvent(worker, task, event);
        }

    }

}
