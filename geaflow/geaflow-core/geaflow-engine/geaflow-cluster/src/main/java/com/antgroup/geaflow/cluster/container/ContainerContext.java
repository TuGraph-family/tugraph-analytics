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

package com.antgroup.geaflow.cluster.container;

import com.antgroup.geaflow.cluster.common.IReliableContext;
import com.antgroup.geaflow.cluster.common.ReliableContainerContext;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IComposeEvent;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.protocol.IHighAvailableEvent;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerContext extends ReliableContainerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerContext.class);

    private List<IEvent> reliableEvents;
    private transient List<IEvent> waitingCheckpointEvents;

    public ContainerContext(int id, Configuration config) {
        super(id, config);
        this.reliableEvents = new ArrayList<>();
        this.waitingCheckpointEvents = new ArrayList<>();
    }

    public ContainerContext(int id, Configuration config, boolean isRecover) {
        this(id, config);
        this.isRecover = isRecover;
    }

    public ContainerContext(int id, Configuration config, boolean isRecover, List<IEvent> reliableEvents) {
        this(id, config, isRecover);
        this.reliableEvents = reliableEvents;
    }

    @Override
    public void load() {
        if (isRecover()) {
            List<IEvent> events = ClusterMetaStore.getInstance(id, config)
                .getEvents();
            if (events == null) {
                LOGGER.info("container {} not found any events to recover", id);
            } else {
                LOGGER.info("container {} recover events {}", id, events);
                reliableEvents = events;
            }
        }
        if (waitingCheckpointEvents == null) {
            waitingCheckpointEvents = new ArrayList<>();
        } else {
            waitingCheckpointEvents.clear();
        }
    }

    public static ContainerContext build(int id, Configuration config, boolean isRecover) {
        List<IEvent> events = ClusterMetaStore.getInstance(id, config).getEvents();
        if (events == null) {
            LOGGER.info("container {} not found any events to recover", id);
            return new ContainerContext(id, config, isRecover);
        }
        LOGGER.info("container {} recover events {}", id, events);
        ContainerContext context = new ContainerContext(id, config, true, events);
        return context;
    }

    public List<IEvent> getReliableEvents() {
        return reliableEvents;
    }

    public synchronized void addEvent(IEvent input) {
        if (input instanceof IHighAvailableEvent) {
            if (((IHighAvailableEvent) input).getHaLevel() == HighAvailableLevel.CHECKPOINT) {
                if (waitingCheckpointEvents == null) {
                    waitingCheckpointEvents = new ArrayList<>();
                }
                if (!waitingCheckpointEvents.contains(input)) {
                    waitingCheckpointEvents.add(input);
                    LOGGER.info("container {} add recoverable event {}", id, input);
                } else {
                    LOGGER.info("container {} already has recoverable event {}", id, input);
                }
            }
        } else if (input.getEventType() == EventType.COMPOSE) {
            IComposeEvent composeEvent = (IComposeEvent) input;
            for (IEvent event : composeEvent.getEventList()) {
                addEvent(event);
            }
        }
    }

    public static class EventCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            ContainerContext containerContext = ((ContainerContext) context);
            if (containerContext.waitingCheckpointEvents == null || containerContext.waitingCheckpointEvents.isEmpty()) {
                LOGGER.info("container {} has no new events to checkpoint", containerContext.getId());
                return;
            }
            List<IEvent> reliableEvents = ClusterMetaStore.getInstance().getEvents();

            if (reliableEvents == null) {
                reliableEvents = containerContext.waitingCheckpointEvents;
            } else {
                for (IEvent event : containerContext.waitingCheckpointEvents) {
                    if (reliableEvents.contains(event)) {
                        LOGGER.info("container {} already has saved recoverable event {}", containerContext.id, event);
                    } else {
                        reliableEvents.add(event);
                    }
                }
            }
            ClusterMetaStore.getInstance().saveEvent(reliableEvents).flush();
            LOGGER.info("container {} checkpoint events {}", containerContext.getId(), reliableEvents);
            containerContext.waitingCheckpointEvents.clear();
        }
    }
}
