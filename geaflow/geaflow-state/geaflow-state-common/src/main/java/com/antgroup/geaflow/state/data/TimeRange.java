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

package com.antgroup.geaflow.state.data;

import com.antgroup.geaflow.utils.math.MathUtil;
import java.io.Serializable;

public class TimeRange implements Comparable<TimeRange>, Serializable {

    private final long start;
    private final long end;

    private TimeRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Return a TimeRange from start(INCLUSIVE) to end(EXCLUSIVE).
     */
    public static TimeRange of(long start, long end) {
        return new TimeRange(start, end);
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public int hashCode() {
        return MathUtil.longToIntWithBitMixing(start + end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeRange range = (TimeRange) o;

        return end == range.end && start == range.start;
    }

    @Override
    public String toString() {
        return String.format("TimeRange{start=%d, end=%d}", start, end);
    }

    /**
     * Returns {@code true} if this range contain the given ts.
     */
    public boolean contain(long ts) {
        return ts >= start && ts < end;
    }

    @Override
    public int compareTo(TimeRange o) {
        return Long.compare(end, o.getEnd());
    }
}
