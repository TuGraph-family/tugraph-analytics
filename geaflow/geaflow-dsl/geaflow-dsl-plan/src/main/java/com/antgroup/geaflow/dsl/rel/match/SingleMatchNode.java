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

package com.antgroup.geaflow.dsl.rel.match;

import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;

public interface SingleMatchNode extends IMatchNode {

    RelNode getInput();

    default SingleMatchNode find(Predicate<SingleMatchNode> condition) {
        if (condition.test(this)) {
            return this;
        }
        if (this.getInput() == null) {
            return null;
        }
        return ((SingleMatchNode) this.getInput()).find(condition);
    }
}
