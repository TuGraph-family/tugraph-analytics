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

package com.antgroup.geaflow.infer.log;

import com.google.common.base.Throwables;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class ProcessErrorOutputLogger implements Runnable {

    private static final String LINE_SIGNAL = "\n";

    private final StringBuilder buffer = new StringBuilder();

    private final InputStream inputStream;

    private final Consumer<String> consumer;

    public ProcessErrorOutputLogger(InputStream inputStream, Consumer<String> consumer) {
        this.inputStream = inputStream;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.append(line).append(LINE_SIGNAL);
                consumer.accept(line);
            }
        } catch (Exception e) {
            consumer.accept(Throwables.getStackTraceAsString(e));
        }
    }

    public String get() {
        return buffer.toString();
    }
}