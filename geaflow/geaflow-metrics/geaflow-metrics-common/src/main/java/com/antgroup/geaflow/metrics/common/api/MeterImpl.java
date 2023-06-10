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

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MovingAverages;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class MeterImpl extends Meter implements com.antgroup.geaflow.metrics.common.api.Meter {

    private final MovingAverages movingAverages;
    private final LongAdder count;
    private final long startTime;
    private final Clock clock;

    public MeterImpl(MovingAverages movingAverages) {
        this(movingAverages, Clock.defaultClock());
    }

    public MeterImpl() {
        this(Clock.defaultClock());
    }

    public MeterImpl(Clock clock) {
        this(new ExponentialMovingAverages(clock), clock);
    }

    public MeterImpl(MovingAverages movingAverages, Clock clock) {
        this.count = new LongAdder();
        this.movingAverages = movingAverages;
        this.clock = clock;
        this.startTime = this.clock.getTick();
    }

    public void mark() {
        this.mark(1L);
    }

    public void mark(long n) {
        this.movingAverages.tickIfNecessary();
        this.count.add(n);
        this.movingAverages.update(n);
    }

    public double getFifteenMinuteRate() {
        this.movingAverages.tickIfNecessary();
        return this.movingAverages.getM15Rate();
    }

    public double getFiveMinuteRate() {
        this.movingAverages.tickIfNecessary();
        return this.movingAverages.getM5Rate();
    }

    public double getMeanRate() {
        if (this.getCount() == 0L) {
            return 0.0D;
        } else {
            double elapsed = (double)(this.clock.getTick() - this.startTime);
            return (double)this.getCount() / elapsed * (double) TimeUnit.SECONDS.toNanos(1L);
        }
    }

    public double getOneMinuteRate() {
        this.movingAverages.tickIfNecessary();
        return this.movingAverages.getM1Rate();
    }

    public long getCount() {
        return this.count.sum();
    }

    public long getCountAndReset() {
        return this.count.sumThenReset();
    }

}
