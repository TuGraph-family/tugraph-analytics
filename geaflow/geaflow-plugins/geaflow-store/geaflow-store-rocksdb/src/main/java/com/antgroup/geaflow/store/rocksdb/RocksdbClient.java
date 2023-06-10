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
import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.store.rocksdb.options.IRocksDBOptions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksdbClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksdbClient.class);

    private final String filePath;
    private final String optionClass;
    private final Configuration config;
    private final List<String> cfList;
    private RocksDB rocksdb;
    private IRocksDBOptions rocksDBOptions;
    private Map<String, ColumnFamilyHandle> handleMap = new HashMap<>();
    private ArrayList<ColumnFamilyDescriptor> descriptors;

    public RocksdbClient(String filePath, List<String> cfList, Configuration config) {
        this.filePath = filePath;
        this.cfList = cfList;
        this.config = config;
        this.optionClass = this.config.getString(RocksdbConfigKeys.ROCKSDB_OPTION_CLASS);
    }

    private void initRocksDbOptions() {
        if (this.rocksDBOptions == null || this.rocksDBOptions.isClosed()) {
            LOGGER.info("rocksdb optionClass {}", optionClass);
            try {
                this.rocksDBOptions = (IRocksDBOptions) Class.forName(optionClass).newInstance();
                this.rocksDBOptions.init(config);
            } catch (Throwable e) {
                LOGGER.error("{} not found", optionClass);
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("class not found"), e);
            }

            if (this.config.getBoolean(RocksdbConfigKeys.ROCKSDB_STATISTICS_ENABLE)) {
                this.rocksDBOptions.enableStatistics();
            }
            if (this.config.getBoolean(StateConfigKeys.STATE_PARANOID_CHECK_ENABLE)) {
                this.rocksDBOptions.enableParanoidCheck();
            }
        }
    }

    public void initDB() {
        File dbFile = new File(filePath);
        if (!dbFile.getParentFile().exists()) {
            try {
                FileUtils.forceMkdir(dbFile.getParentFile());
            } catch (IOException e) {
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("create file error"), e);
            }
        }

        if (rocksdb == null) {
            initRocksDbOptions();
            LOGGER.info("ThreadId {}, buildDB {}", Thread.currentThread().getId(), filePath);
            int ttl = this.config.getInteger(RocksdbConfigKeys.ROCKSDB_TTL_SECOND);
            this.descriptors = new ArrayList<>();
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            List<Integer> ttls = new ArrayList<>();
            for (String name : cfList) {
                descriptors.add(new ColumnFamilyDescriptor(name.getBytes(), rocksDBOptions.buildFamilyOptions()));
                ttls.add(ttl);
            }

            try {
                rocksdb = TtlDB.open(rocksDBOptions.getDbOptions(), this.filePath, descriptors,
                    handles, ttls, false);
            } catch (Exception e) {
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("open rocksdb error"), e);
            }
            for (int i = 0; i < cfList.size(); i++) {
                handleMap.put(cfList.get(i), handles.get(i));
            }
        }
    }

    public Map<String, ColumnFamilyHandle> getColumnFamilyHandleMap() {
        return handleMap;
    }

    public void flush() {
        try {
            this.rocksdb.flush(rocksDBOptions.getFlushOptions());
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb compact error"), e);
        }
    }


    public void compact() {
        try {
            this.rocksdb.compactRange();
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb compact error"), e);
        }
    }

    public void checkpoint(String path) {
        Checkpoint checkpoint = Checkpoint.create(rocksdb);
        FileUtils.deleteQuietly(new File(path));
        try {
            checkpoint.createCheckpoint(path);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb chk error"), e);
        }
        checkpoint.close();
    }

    public void write(WriteBatch writeBatch) {
        try {
            this.rocksdb.write(rocksDBOptions.getWriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public void write(String cf, byte[] key, byte[] value) {
        try {
            this.rocksdb.put(handleMap.get(cf), key, value);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public void write(String cf, List<Tuple<byte[], byte[]>> list) {
        try {
            WriteBatch writeBatch = new WriteBatch();
            for (Tuple<byte[], byte[]> tuple: list) {
                writeBatch.put(handleMap.get(cf), tuple.f0, tuple.f1);
            }
            this.rocksdb.write(rocksDBOptions.getWriteOptions(), writeBatch);
            writeBatch.clear();
            writeBatch.close();
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public byte[] get(String cf, byte[] key) {
        try {
            return this.rocksdb.get(handleMap.get(cf), key);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb get error"), e);
        }
    }

    public void delete(String cf, byte[] key) {
        try {
            this.rocksdb.delete(handleMap.get(cf), key);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb delete error"), e);
        }
    }

    public RocksIterator getIterator(String cf) {
        return this.rocksdb.newIterator(handleMap.get(cf));
    }

    public void close() {
        if (rocksdb != null) {
            this.rocksdb.close();
            this.rocksDBOptions.close();
            this.descriptors.forEach(d -> d.getOptions().close());
            this.rocksDBOptions = null;
            this.rocksdb = null;
        }
    }

    public void drop() {
        close();
        FileUtils.deleteQuietly(new File(this.filePath));
    }
}
