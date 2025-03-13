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

package com.antgroup.geaflow.example.k8s;

import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.example.stream.StreamWordCountPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnBoundedStreamWordCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnBoundedStreamWordCount.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentFactory.onK8SEnvironment(args);

        StreamWordCountPipeline pipeline = new StreamWordCountPipeline();
        pipeline.submit(environment);
    }
}
