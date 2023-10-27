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

package com.antgroup.geaflow.cluster.system;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerSnapshot;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.pipeline.Pipeline;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMetaStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMetaStore.class);

    private static final String CLUSTER_META_NAMESPACE_LABEL = "framework";
    private static final String CLUSTER_NAMESPACE_PREFIX = "cluster";
    private static final String OFFSET_NAMESPACE = "offset";

    private static ClusterMetaStore INSTANCE;

    private final int componentId;
    private final Configuration configuration;

    private IClusterMetaKVStore<String, Object> backend;
    private IClusterMetaKVStore<String, Object> offsetBackend;

    private ClusterMetaStore(int id, Configuration configuration) {
        this.componentId = id;
        this.configuration = configuration;
        String clusterId = configuration.getString(CLUSTER_ID);
        String storeKey = String.format("%s/%s/%s", CLUSTER_META_NAMESPACE_LABEL, CLUSTER_NAMESPACE_PREFIX, clusterId);
        this.backend = ClusterMetaStoreFactory.create(storeKey, id, configuration);
        LOGGER.info("create ClusterMetaStore, store key {}, id {}", storeKey, id);
    }

    public static synchronized void init(int id, Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new ClusterMetaStore(id, configuration);
        }
    }

    public static ClusterMetaStore getInstance(int id, Configuration configuration) {
        if (INSTANCE == null) {
            init(id, configuration);
        }
        return INSTANCE;
    }

    public static ClusterMetaStore getInstance() {
        return INSTANCE;
    }

    public static synchronized void close() {
        LOGGER.info("close ClusterMetaStore");
        if (INSTANCE != null) {
            INSTANCE.backend.close();
            if (INSTANCE.offsetBackend != null) {
                INSTANCE.offsetBackend.close();
            }
            INSTANCE = null;
        }
    }

    private IClusterMetaKVStore<String, Object> getOffsetBackend() {
        if (offsetBackend == null) {
            synchronized (ClusterMetaStore.class) {
                String storeKey = String.format("%s/%s", CLUSTER_META_NAMESPACE_LABEL, OFFSET_NAMESPACE);
                offsetBackend = ClusterMetaStoreFactory.create(storeKey, componentId, configuration);
                LOGGER.info("create ClusterMetaStore, store key {}, id {}", storeKey, componentId);
            }
        }
        return offsetBackend;
    }

    public ClusterMetaStore savePipeline(Pipeline pipeline) {
        save(ClusterMetaKey.PIPELINE, pipeline);
        return this;
    }

    public ClusterMetaStore savePipelineTasks(List<Integer> taskIndices) {
        save(ClusterMetaKey.PIPELINE_TASKS, taskIndices);
        return this;
    }

    public void saveWindowId(Long windowId) {
        IClusterMetaKVStore<String, Object> offsetBackend = getOffsetBackend();
        offsetBackend.put(ClusterMetaKey.WINDOW_ID.name(), windowId);
        offsetBackend.flush();
    }

    public ClusterMetaStore saveCycle(Object cycle) {
        save(ClusterMetaKey.CYCLE, cycle);
        return this;
    }
    
    public ClusterMetaStore saveEvent(List<IEvent> event) {
        save(ClusterMetaKey.EVENTS, event);
        return this;
    }

    public ClusterMetaStore saveWorkers(WorkerSnapshot workers) {
        save(ClusterMetaKey.WORKERS, workers);
        return this;
    }

    public ClusterMetaStore saveContainerIds(Map<Integer, String> containerIds) {
        save(ClusterMetaKey.CONTAINER_IDS, containerIds);
        return this;
    }

    public ClusterMetaStore saveDriverIds(Map<Integer, String> driverIds) {
        save(ClusterMetaKey.DRIVER_IDS, driverIds);
        return this;
    }

    public ClusterMetaStore saveMaxContainerId(int containerId) {
        save(ClusterMetaKey.MAX_CONTAINER_ID, containerId);
        return this;
    }

    public Pipeline getPipeline() {
        return get(ClusterMetaKey.PIPELINE);
    }

    public List<Integer> getPipelineTasks() {
        return get(ClusterMetaKey.PIPELINE_TASKS);
    }

    public Long getWindowId() {
        IClusterMetaKVStore<String, Object> offsetBackend = getOffsetBackend();
        return (Long) offsetBackend.get(ClusterMetaKey.WINDOW_ID.name());
    }

    public Object getCycle() {
        return get(ClusterMetaKey.CYCLE);
    }

    public List<IEvent> getEvents() {
        return get(ClusterMetaKey.EVENTS);
    }

    public WorkerSnapshot getWorkers() {
        return get(ClusterMetaKey.WORKERS);
    }

    public int getMaxContainerId() {
        return get(ClusterMetaKey.MAX_CONTAINER_ID);
    }

    public Map<Integer, String> getContainerIds() {
        return get(ClusterMetaKey.CONTAINER_IDS);
    }

    public Map<Integer, String> getDriverIds() {
        return get(ClusterMetaKey.DRIVER_IDS);
    }

    public void flush() {
        backend.flush();
    }

    public void clean() {
        // TODO Clean namespace directly from backend.
    }

    private <T> void save(ClusterMetaKey key, T value) {
        backend.put(key.name(), value);
    }

    private <T> void save(String key, T value) {
        backend.put(key, value);
    }

    private <T> T get(ClusterMetaKey key) {
        return (T) backend.get(key.name());
    }

    private <T> T get(String key) {
        return (T) backend.get(key);
    }

    public enum ClusterMetaKey {

        PIPELINE,
        PIPELINE_TASKS,
        WINDOW_ID,
        CYCLE,
        EVENTS,
        WORKERS,
        CONTAINER_IDS,
        DRIVER_IDS,
        MAX_CONTAINER_ID
    }
}
