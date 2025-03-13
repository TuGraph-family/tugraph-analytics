/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.console.core.service.llm;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowLLMType;
import org.springframework.stereotype.Component;

@Component
public class LLMClientFactory {

    public LLMClient getLLMClient(GeaflowLLMType type) {
        switch (type) {
            case OPEN_AI:
                return OpenAiClient.getInstance();
            case LOCAL:
                return LocalClient.getInstance();
            default:
                throw new GeaflowException("Unsupported LLM type, {}", type);
        }

    }
}
