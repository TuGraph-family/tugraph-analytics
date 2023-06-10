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

package com.antgroup.geaflow.dsl.common.binary.decoder;

import com.antgroup.geaflow.dsl.common.binary.DecoderFactory;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.DefaultPath;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import java.util.ArrayList;
import java.util.List;

public class DefaultPathDecoder implements PathDecoder {

    private final IBinaryDecoder[] binaryDecoders;

    public DefaultPathDecoder(PathType pathType) {
        this.binaryDecoders = new IBinaryDecoder[pathType.size()];
        List<TableField> pathFields = pathType.getFields();
        for (int i = 0; i < pathFields.size(); i++) {
            TableField field = pathFields.get(i);
            binaryDecoders[i] = DecoderFactory.createDecoder(field.getType());
        }
    }

    @Override
    public Path decode(Path rowPath) {
        List<Row> pathNodes = rowPath.getPathNodes();
        List<Row> decodePathNodes = new ArrayList<>(pathNodes.size());
        for (int i = 0; i < pathNodes.size(); i++) {
            decodePathNodes.add(binaryDecoders[i].decode(pathNodes.get(i)));
        }
        return new DefaultPath(decodePathNodes);
    }
}
