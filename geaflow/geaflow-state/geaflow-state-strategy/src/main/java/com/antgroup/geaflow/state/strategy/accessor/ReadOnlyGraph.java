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

package com.antgroup.geaflow.state.strategy.accessor;

import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.action.IAction;
import com.antgroup.geaflow.state.action.StateActionContext;
import com.antgroup.geaflow.state.action.close.CloseAction;
import com.antgroup.geaflow.state.action.drop.DropAction;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.graph.StateMode;
import com.antgroup.geaflow.state.manage.LoadOption;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.context.StoreContext;
import com.antgroup.geaflow.view.meta.ViewMetaBookKeeper;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadOnlyGraph<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadOnlyGraph.class);

    protected StateContext context;
    protected IStoreBuilder storeBuilder;
    protected ViewMetaBookKeeper viewMetaBookKeeper;

    protected ScheduledExecutorService syncExecutor;
    protected long currentVersion;
    protected Throwable warmupException;
    protected AtomicBoolean initialized;
    // InUseGraphStore is the store in using.
    protected IBaseStore inUseGraphStore;
    // LatestGraphStore is the store referring to the latest version.
    protected IBaseStore latestGraphStore;
    // LazyCloseGraphStore is the store in querying while current store is switching.
    protected IBaseStore lazyCloseGraphStore;
    protected boolean enableRecoverLatestVersion;
    protected boolean enableStateBackgroundSync;
    protected int syncGapMs;

    public void init(StateContext context, IStoreBuilder storeBuilder) {
        Preconditions.checkArgument(context.getStateMode() == StateMode.RDONLY);
        this.context = context;
        this.storeBuilder = storeBuilder;
        this.viewMetaBookKeeper = new ViewMetaBookKeeper(context.getName(), context.getConfig());
        this.enableRecoverLatestVersion =
            context.getConfig().getBoolean(StateConfigKeys.STATE_RECOVER_LATEST_VERSION_ENABLE);
        this.enableStateBackgroundSync =
            context.getConfig().getBoolean(StateConfigKeys.STATE_BACKGROUND_SYNC_ENABLE);
        this.syncGapMs = context.getConfig().getInteger(StateConfigKeys.STATE_SYNC_GAP_MS);
        this.initialized = new AtomicBoolean(false);
        if (this.enableStateBackgroundSync) {
            this.enableRecoverLatestVersion = true;
            LOGGER.info("initialize background sync service");
            this.syncExecutor =
                Executors.newSingleThreadScheduledExecutor(ThreadUtil.namedThreadFactory(false,
                    Thread.currentThread().getName() + "read-only-background-sync-" + context.getShardId()));
            this.startStateSyncService();
        }
    }

    protected List<ActionType> allowActionTypes() {
        return Arrays.asList(ActionType.RECOVER, ActionType.LOAD, ActionType.DROP, ActionType.CLOSE);
    }

    public void doStoreAction(int shard, ActionType actionType, ActionRequest request) {
        if (actionType == ActionType.DROP || actionType == ActionType.CLOSE) {
            IAction action = actionType == ActionType.DROP ? new DropAction() : new CloseAction();
            StateActionContext stateActionContext = new StateActionContext(latestGraphStore, context.getConfig());
            action.init(stateActionContext);
            action.apply(request);
            if (enableStateBackgroundSync) {
                this.syncExecutor.shutdown();
            }
        }
        if (actionType == ActionType.RECOVER) {
            long version = (long) request.getRequest();
            if (!enableStateBackgroundSync) {
                recover(version);
            }
        } else if (actionType == ActionType.LOAD) {
            LOGGER.info("wait async background sync to be finished");
            LoadOption option = (LoadOption) request.getRequest();
            if (option.getKeyGroup() != null && !option.getKeyGroup().contains(shard)) {
                return;
            }
            if (enableStateBackgroundSync) {
                while (!this.initialized.get()) {
                    if (warmupException != null) {
                        throw new GeaflowRuntimeException("warmup error", this.warmupException);
                    }
                    SleepUtils.sleepMilliSecond(1000);
                }
            } else {
                recover(option.getCheckPointId());
            }
        }
    }

    protected void recover(long version) {
        if (enableRecoverLatestVersion) {
            try {
                version = viewMetaBookKeeper.getLatestViewVersion(context.getName());
            } catch (Throwable t) {
                throw new GeaflowRuntimeException("failed to get latest version", t);
            }
        }
        if (latestGraphStore == null) {
            createReadOnlyState(version);
        } else {
            updateVersion(version);
        }
    }

    protected void startStateSyncService() {
        this.syncExecutor.scheduleAtFixedRate(() -> {
            try {
                final long start = System.currentTimeMillis();
                long latestVersion = viewMetaBookKeeper.getLatestViewVersion(context.getName());
                Preconditions.checkArgument(latestVersion > 0);
                if (latestVersion != currentVersion) {
                    createReadOnlyState(latestVersion);
                    currentVersion = latestVersion;
                } else {
                    LOGGER.info("don't need recover, current version {} latest version {}", currentVersion, latestVersion);
                }
                // Try to update in-use connection.
                getStore();
                LOGGER.info("background sync finished cost {}", System.currentTimeMillis() - start);
                this.initialized.set(true);
            } catch (Throwable t) {
                if (!initialized.get()) {
                    this.warmupException = t;
                }
                LOGGER.error("background sync error", t);
            }
        }, 0, syncGapMs, TimeUnit.MILLISECONDS);
    }

    public IBaseStore getStore() {
        if (enableStateBackgroundSync) {
            if (latestGraphStore != inUseGraphStore) {
                synchronized (ReadOnlyStaticGraphAccessor.class) {
                    if (latestGraphStore == inUseGraphStore) {
                        return inUseGraphStore;
                    }
                    if (lazyCloseGraphStore != null) {
                        lazyCloseGraphStore.close();
                    }
                    lazyCloseGraphStore = inUseGraphStore;
                    inUseGraphStore = latestGraphStore;
                }
            }
            return inUseGraphStore;
        } else {
            if (latestGraphStore == null) {
                LOGGER.warn("create graph store is null, shardId {}, keyGroup {}",
                    context.getShardId(), context.getKeyGroup());
            }
            return latestGraphStore;
        }
    }

    protected void createReadOnlyState(long version) {
        LOGGER.info("create new read only state, state index {} version {} backend type {}",
            context.getShardId(), version, context.getStoreType());
        IBaseStore graphStoreTmp =
            storeBuilder.getStore(this.context.getDataModel(), context.getConfig());
        GraphStateDescriptor<K, VV, EV> desc = (GraphStateDescriptor<K, VV, EV>) context.getDescriptor();

        StoreContext storeContext = new StoreContext(context.getName())
            .withConfig(context.getConfig())
            .withMetricGroup(context.getMetricGroup())
            .withDataSchema(desc.getGraphSchema())
            .withName(context.getName())
            .withShardId(context.getShardId());
        graphStoreTmp.init(storeContext);
        graphStoreTmp.recovery(version);
        latestGraphStore = graphStoreTmp;
    }

    protected void updateVersion(long version) {
        LOGGER.info("update read only state, state index {} version {}", context.getShardId(), version);
        latestGraphStore.recovery(version);
    }
}
