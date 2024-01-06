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

package com.antgroup.geaflow.dsl.connector.file.source.format;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.IOException;
import java.util.Iterator;

public interface FileFormat<T> {

    String getFormat();

    void init(Configuration tableConf, TableSchema tableSchema, FileSplit split) throws IOException;

    Iterator<T> batchRead() throws IOException;

    void close() throws IOException;

    TableDeserializer<T> getDeserializer();
}
