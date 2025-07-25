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

package org.apache.geaflow.common.config;

import java.io.Serializable;

public class ConfigKeys implements Serializable {

    public static ConfigKeyBuilder key(String key) {
        return new ConfigKeyBuilder(key);
    }

    public static class ConfigKeyBuilder {

        private final String key;

        public ConfigKeyBuilder(String key) {
            this.key = key;
        }

        public ConfigKey defaultValue(Object defaultValue) {
            return new ConfigKey(key, defaultValue);
        }

        public ConfigKey noDefaultValue() {
            return new ConfigKey(key);
        }

    }

}
