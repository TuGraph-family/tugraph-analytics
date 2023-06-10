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

package com.antgroup.geaflow.state.action.hook;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.IPersistentIO;
import com.antgroup.geaflow.file.PersistentIOBuilder;
import com.antgroup.geaflow.state.action.ActionRequest;
import com.antgroup.geaflow.state.action.ActionType;
import com.antgroup.geaflow.state.context.StateContext;
import com.antgroup.geaflow.state.strategy.accessor.IAccessor;
import com.antgroup.geaflow.store.ILocalStore;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.math.IntMath;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScaleHook implements ActionHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScaleHook.class);

    private Map<Integer, IAccessor> accessorMap;
    private KeyGroup shardGroup;
    private int totalShardNum;
    private ScaleManager scaleManager;

    @Override
    public void init(StateContext context, Map<Integer, IAccessor> accessorMap) {
        this.shardGroup = context.getKeyGroup();
        this.totalShardNum = context.getTotalShardNum();
        this.accessorMap = accessorMap;
        this.scaleManager = new ScaleManager(context.getName(), context.getConfig(), this.totalShardNum);
    }

    @Override
    public void doStoreAction(ActionType actionType, ActionRequest request) {
        // pseudo code
        // if archive and shard == 0, upload shardNum and version.
        // if recover, checkNeedScale
        //    if needScale, set recoverShard

        if (this.shardGroup.getStartKeyGroup() == 0 && actionType == ActionType.ARCHIVE) {
            this.scaleManager.tryStoreShardNum((Long)request.getRequest());
        }
        if (actionType == ActionType.RECOVER && scaleManager.needScale((Long)request.getRequest())) {
            for (Entry<Integer, IAccessor> entry: this.accessorMap.entrySet()) {
                int recoverShardId = scaleManager.getRecoverShardId(entry.getKey());
                ((ILocalStore)entry.getValue().getStore()).initShardId(recoverShardId);
            }
        }
    }

    public static class ScaleManager {

        private static final String PARTITION_HEADER = "shardNum#";
        private static final String PARTITION_FILE_FORMAT = PARTITION_HEADER + "%d#%d";
        private static final int PARTITION_HEADER_LEN = PARTITION_HEADER.length();
        private final IPersistentIO persistIO;
        private boolean hasStored = false;
        private final String remotePath;
        private final int shardNum;
        private int lastShardNum = -1;

        public ScaleManager(String name, Configuration configuration, int shardNum) {
            this.persistIO = PersistentIOBuilder.build(configuration);
            String jobName = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
            String root = configuration.getString(FileConfigKeys.ROOT);
            this.remotePath = Paths.get(root, jobName, name).toString();
            this.shardNum = shardNum;
        }

        public boolean needScale(long version) {
            this.lastShardNum = getShardNumByVersion(version);
            if (lastShardNum == 0) {
                return false;
            }
            Preconditions.checkArgument(shardNum >= lastShardNum,
                "partitionNum %s, lastPartitionNum %s", shardNum, lastShardNum);
            return shardNum > lastShardNum;
        }

        public int getRecoverShardId(int shardId) {
            Preconditions.checkArgument(shardNum % lastShardNum == 0
                && IntMath.isPowerOfTwo(shardNum / lastShardNum),
                "shardNum %s, lastShardNum %s", shardId, lastShardNum);

            int recoverShard = shardId % lastShardNum;
            LOGGER.info("auto scale state shard {} recover {}", shardId, recoverShard);
            return recoverShard;
        }

        public void tryStoreShardNum(long version) {
            this.lastShardNum = getShardNumByVersion(version);
            if (this.shardNum > this.lastShardNum && !hasStored) {
                try {
                    this.persistIO.createNewFile(new Path(this.remotePath,
                        String.format(PARTITION_FILE_FORMAT, shardNum, version)));
                    hasStored = true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private int getShardNumByVersion(List<String> fileNames, long version) {
            TreeMap<Long, Integer> treeMap = new TreeMap<>();
            for (String fileName: fileNames) {
                if (fileName.startsWith(PARTITION_HEADER)) {
                    List<String> l = Splitter.on('#')
                        .splitToList(fileName.substring(PARTITION_HEADER_LEN));
                    Preconditions.checkArgument(l.size() == 2);
                    int shardNum = Integer.parseInt(l.get(0));
                    long tmpVersion = Long.parseLong(l.get(1));
                    if (tmpVersion <= version) {
                        treeMap.put(tmpVersion, shardNum);
                    }
                }
            }
            Entry<Long, Integer> entry = treeMap.floorEntry(version);
            if (entry != null) {
                return entry.getValue();
            }
            return 0;
        }

        public int getShardNumByVersion(long version) {
            if (lastShardNum >= 0) {
                return lastShardNum;
            }
            List<String> list;
            try {
                if (!this.persistIO.exists(new Path(this.remotePath))) {
                    return 0;
                }
                list = this.persistIO.listFile(new Path(this.remotePath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return getShardNumByVersion(list, version);
        }
    }
}
