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

package com.antgroup.geaflow.dsl.runtime.function.table;

import com.antgroup.geaflow.dsl.common.binary.decoder.DefaultRowDecoder;
import com.antgroup.geaflow.dsl.common.binary.decoder.RowDecoder;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.StructType;

public class TableDecodeFunctionImpl implements TableDecodeFunction {

    private final RowDecoder rowDecoder;

    public TableDecodeFunctionImpl(StructType rowType) {
        this.rowDecoder = new DefaultRowDecoder(rowType);
    }

    @Override
    public Row decode(Row row) {
        return rowDecoder.decode(row);
    }
}
