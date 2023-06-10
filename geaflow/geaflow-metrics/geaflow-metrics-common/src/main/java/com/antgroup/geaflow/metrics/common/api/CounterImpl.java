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

package com.antgroup.geaflow.metrics.common.api;

import com.codahale.metrics.Counter;
import java.util.concurrent.atomic.LongAdder;

public class CounterImpl extends Counter implements
    com.antgroup.geaflow.metrics.common.api.Counter {

    private final LongAdder count = new LongAdder();

    public CounterImpl() {
    }

    public void inc() {
        this.inc(1L);
    }

    public void inc(long n) {
        this.count.add(n);
    }

    public void dec() {
        this.dec(1L);
    }

    public void dec(long n) {
        this.count.add(-n);
    }

    public long getCount() {
        return this.count.sum();
    }

    public long getCountAndReset() {
        return this.count.sumThenReset();
    }

}
