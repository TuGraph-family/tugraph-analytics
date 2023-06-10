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

package com.antgroup.geaflow.common.shuffle;

import java.io.Serializable;

public class ShuffleDescriptor implements Serializable {

    public static final ShuffleDescriptor BATCH = new ShuffleDescriptor(DataExchangeMode.BATCH);
    public static final ShuffleDescriptor PIPELINE = new ShuffleDescriptor(DataExchangeMode.PIPELINE);

    // Check whether enable cache.
    private boolean cacheEnabled;

    // Data exchange mode between tasks.
    private DataExchangeMode exchangeMode;

    public ShuffleDescriptor() {
        this(DataExchangeMode.PIPELINE);
    }

    public ShuffleDescriptor(DataExchangeMode exchangeMode) {
        this.cacheEnabled = false;
        this.exchangeMode = exchangeMode;
    }

    public ShuffleDescriptor(DataExchangeMode exchangeMode, boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
        this.exchangeMode = exchangeMode;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public DataExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    public void setExchangeMode(DataExchangeMode exchangeMode) {
        this.exchangeMode = exchangeMode;
    }

    public ShuffleDescriptor clone() {
        ShuffleDescriptor shuffleDescriptor = new ShuffleDescriptor();
        shuffleDescriptor.setCacheEnabled(this.cacheEnabled);
        shuffleDescriptor.setExchangeMode(this.exchangeMode);

        return shuffleDescriptor;
    }
}
