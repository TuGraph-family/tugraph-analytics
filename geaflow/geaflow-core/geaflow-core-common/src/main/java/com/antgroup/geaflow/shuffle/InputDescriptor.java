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

package com.antgroup.geaflow.shuffle;

import com.antgroup.geaflow.shuffle.desc.IInputDesc;
import java.io.Serializable;
import java.util.Map;

public class InputDescriptor implements Serializable {

    private final Map<Integer, IInputDesc<?>> inputDescMap;

    public InputDescriptor(Map<Integer, IInputDesc<?>> inputDescMap) {
        this.inputDescMap = inputDescMap;
    }

    public Map<Integer, IInputDesc<?>> getInputDescMap() {
        return inputDescMap;
    }

}
