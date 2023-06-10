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

package com.antgroup.geaflow.store.memory;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class GraphJMHRunner {

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
            // import test class.
            .include(StringMapGraphJMH.class.getSimpleName())
            .include(IntMapGraphJMH.class.getSimpleName())
            .include(StringCSRMapGraphJMH.class.getSimpleName())
            .include(IntCSRMapGraphJMH.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }

}
