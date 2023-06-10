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

package com.antgroup.geaflow.dsl.connector.hive.adapter;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.hive.common.HiveVersionAnnotation;

public class HiveVersionAdapters {

    public static final String HIVE_230 = "2.3.0";

    public static final String HIVE_231 = "2.3.1";

    public static final String HIVE_232 = "2.3.2";

    public static final String HIVE_233 = "2.3.3";

    public static final String HIVE_234 = "2.3.4";

    public static final String HIVE_235 = "2.3.5";

    public static final String HIVE_236 = "2.3.6";

    public static final String HIVE_237 = "2.3.7";

    public static final String HIVE_239 = "2.3.9";

    public static HiveVersionAdapter get() {
        Package myPackage = HiveVersionAnnotation.class.getPackage();
        HiveVersionAnnotation version = myPackage.getAnnotation(HiveVersionAnnotation.class);
        switch (version.version()) {
            case HIVE_230:
            case HIVE_231:
            case HIVE_232:
            case HIVE_233:
            case HIVE_234:
            case HIVE_235:
            case HIVE_236:
            case HIVE_237:
            case HIVE_239:
                return new Hive23Adapter(version.version());
            default:
                throw new GeaFlowDSLException("Hive version: {} is not supported.", version.version());
        }
    }
}
