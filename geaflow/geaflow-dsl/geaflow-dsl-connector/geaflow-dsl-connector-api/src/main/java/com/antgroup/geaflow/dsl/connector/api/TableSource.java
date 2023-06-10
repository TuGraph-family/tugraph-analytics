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

package com.antgroup.geaflow.dsl.connector.api;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Interface for table source.
 */
public interface TableSource extends Serializable {

    /**
     * The init method for compile time.
     */
    void init(Configuration tableConf, TableSchema tableSchema);

    /**
     * The init method for runtime.
     */
    void open(RuntimeContext context);

    /**
     * List all the partitions for the source.
     */
    List<Partition> listPartitions();

    /**
     * Returns the {@link TableDeserializer} for the source to convert data read from
     * the source to {@link Row}.
     */
    <IN> TableDeserializer<IN> getDeserializer(Configuration conf);

    /**
     * Fetch data for the partition from start offset. if the windowSize is -1, it represents an
     * all-window which will read all the data from the source, else return window size for data.
     */
    <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, long windowSize) throws IOException;

    /**
     * The close callback for the job finish the execution.
     */
    void close();
}
