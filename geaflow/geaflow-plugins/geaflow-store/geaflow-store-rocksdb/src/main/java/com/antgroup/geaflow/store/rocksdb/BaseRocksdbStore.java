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

package com.antgroup.geaflow.store.rocksdb;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.ILocalStore;
import com.antgroup.geaflow.store.context.StoreContext;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseRocksdbStore implements IBaseStore, ILocalStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRocksdbStore.class);

    protected Configuration config;
    protected String rocksdbPath;
    protected String remotePath;
    protected RocksdbClient rocksdbClient;
    protected RocksdbPersistClient persistClient;
    protected long keepChkNum;

    protected String root;
    protected String jobName;
    protected StoreContext storeContext;
    protected int shardId;
    protected long recoveryVersion = -1;

    @Override
    public void init(StoreContext storeContext) {
        this.storeContext = storeContext;
        this.config = storeContext.getConfig();
        this.shardId = storeContext.getShardId();

        String workerPath = this.config.getString(ExecutionConfigKeys.JOB_WORK_PATH);
        this.jobName = this.config.getString(ExecutionConfigKeys.JOB_APP_NAME);

        this.rocksdbPath = Paths.get(workerPath, jobName, storeContext.getName(),
            Integer.toString(shardId)).toString();

        this.root = this.config.getString(FileConfigKeys.ROOT);

        this.remotePath = getRemotePath().toString();
        this.persistClient = new RocksdbPersistClient(this.config);
        long chkRate = this.config.getLong(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT);
        this.keepChkNum =
            Math.max(this.config.getInteger(StateConfigKeys.STATE_ARCHIVED_VERSION_NUM), chkRate * 2);

        this.rocksdbClient = new RocksdbClient(rocksdbPath, getCfList(), config);
        LOGGER.info("ThreadId {}, BaseRocksdbStore initDB",
            Thread.currentThread().getId());
        this.rocksdbClient.initDB();
    }

    protected abstract List<String> getCfList();

    @Override
    public void archive(long version) {
        flush();
        String chkPath = RocksdbConfigKeys.getChkPath(this.rocksdbPath, version);
        rocksdbClient.checkpoint(chkPath);
        // sync file
        try {
            persistClient.archive(version, chkPath, remotePath, keepChkNum);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("archive fail"), e);
        }
    }

    @Override
    public void recovery(long version) {
        if (version <= recoveryVersion) {
            LOGGER.info("shardId {} recovery version {} <= last recovery version {}, ignore",
                shardId, version, recoveryVersion);
            return;
        }
        drop();
        String chkPath = RocksdbConfigKeys.getChkPath(this.rocksdbPath, version);
        String recoverPath = remotePath;
        boolean isScale = shardId != storeContext.getShardId();
        if (isScale) {
            recoverPath = getRemotePath().toString();
        }
        try {
            persistClient.recover(version, this.rocksdbPath, chkPath, recoverPath);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("recover fail"), e);
        }
        if (isScale) {
            persistClient.clearFileInfo();
            shardId = storeContext.getShardId();
        }
        this.rocksdbClient.initDB();
        recoveryVersion = version;
    }

    protected Path getRemotePath() {
        return Paths.get(root, jobName, storeContext.getName(),
            Integer.toString(shardId));
    }

    @Override
    public long recoveryLatest() {
        long chkId = persistClient.getLatestCheckpointId(remotePath);
        if (chkId > 0) {
            recovery(chkId);
        }
        return chkId;
    }

    @Override
    public void compact() {
        this.rocksdbClient.compact();
    }

    @Override
    public void flush() {
        this.rocksdbClient.flush();
    }

    @Override
    public void close() {
        this.rocksdbClient.close();
    }

    @Override
    public void drop() {
        rocksdbClient.drop();
    }

    @Override
    public void initShardId(int shardId) {
        this.shardId = shardId;
    }
}
