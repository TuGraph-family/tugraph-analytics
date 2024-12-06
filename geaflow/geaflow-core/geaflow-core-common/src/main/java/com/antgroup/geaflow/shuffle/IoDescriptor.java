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

import java.io.Serializable;

public class IoDescriptor implements Serializable {

    // A InputDescriptor that represents the upstream input descriptor info.
    private final InputDescriptor inputDescriptor;
    // A OutputDescriptor that represents the downstream output descriptor info.
    private final OutputDescriptor outputDescriptor;

    public IoDescriptor(InputDescriptor inputDescriptor, OutputDescriptor outputDescriptor) {
        this.inputDescriptor = inputDescriptor;
        this.outputDescriptor = outputDescriptor;
    }

    public InputDescriptor getInputDescriptor() {
        return inputDescriptor;
    }

    public OutputDescriptor getOutputDescriptor() {
        return outputDescriptor;
    }

}
