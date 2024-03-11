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

package com.antgroup.geaflow.infer.exchange.serialize;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;

public class ComplexNumberConstructor extends AnyClassConstructor {
    private static final String NAN = "nan";

    public ComplexNumberConstructor() {
        super(ComplexNumber.class);
    }

    protected Object initClassImpl(Constructor<?> cons, Object[] args) throws Exception {
        if (this.type == BigDecimal.class && args.length == 1) {
            String nan = (String) args[0];
            if (nan.equalsIgnoreCase(NAN)) {
                return Double.NaN;
            }
        }
        return cons.newInstance(args);
    }
}
