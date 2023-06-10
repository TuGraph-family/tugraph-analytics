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

package com.antgroup.geaflow.file;

import com.antgroup.geaflow.common.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public interface IPersistentIO {

    void init(Configuration userConfig);

    List<String> listFile(Path path) throws IOException;

    boolean exists(Path path) throws IOException;

    void delete(Path path, boolean recursive) throws IOException;

    boolean rename(Path from, Path to) throws IOException;

    boolean createNewFile(Path path) throws IOException;

    void copyFromLocalFile(Path local, Path remote) throws IOException;

    void copyToLocalFile(Path remote, Path local) throws IOException;

    long getRemoteFileSize(Path path) throws IOException;

    long getFileCount(Path path) throws IOException;

    FileInfo getFileInfo(Path path) throws IOException;

    FileInfo[] listStatus(Path path, PathFilter filter) throws IOException;

    FileInfo[] listStatus(Path path) throws IOException;

    InputStream open(Path path) throws IOException;

    void close() throws IOException;

    PersistentType getPersistentType();
}
