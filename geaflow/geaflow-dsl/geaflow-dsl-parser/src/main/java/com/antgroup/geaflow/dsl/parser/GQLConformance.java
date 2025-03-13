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

package com.antgroup.geaflow.dsl.parser;

import org.apache.calcite.sql.validate.SqlConformance;

public class GQLConformance implements SqlConformance {

    public static GQLConformance INSTANCE = new GQLConformance();

    @Override
    public boolean isLiberal() {
        return false;
    }

    @Override
    public boolean isGroupByAlias() {
        return true;
    }

    @Override
    public boolean isGroupByOrdinal() {
        return false;
    }

    @Override
    public boolean isHavingAlias() {
        return false;
    }

    @Override
    public boolean isPercentRemainderAllowed() {
        return true;
    }

    @Override
    public boolean allowNiladicParentheses() {
        return false;
    }

    @Override
    public boolean allowExplicitRowValueConstructor() {
        return false;
    }

    @Override
    public boolean allowExtend() {
        return false;
    }

    @Override
    public boolean isLimitStartCountAllowed() {
        return false;
    }

    @Override
    public boolean allowGeometry() {
        return false;
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }

    @Override
    public boolean allowExtendedTrim() {
        return false;
    }

    @Override
    public boolean isSortByOrdinal() {
        return false;
    }

    @Override
    public boolean isSortByAlias() {
        return true;
    }

    @Override
    public boolean isSortByAliasObscures() {
        return false;
    }

    @Override
    public boolean isFromRequired() {
        return false;
    }

    @Override
    public boolean isBangEqualAllowed() {
        return true;
    }

    @Override
    public boolean isMinusAllowed() {
        return false;
    }

    @Override
    public boolean isApplyAllowed() {
        return false;
    }

    @Override
    public boolean isInsertSubsetColumnsAllowed() {
        return false;
    }
}
