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

package com.antgroup.geaflow.dsl.sqlnode;

import com.antgroup.geaflow.dsl.operator.SqlMatchEdgeOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class SqlMatchEdge extends SqlMatchNode {

    private final EdgeDirection direction;

    private final int minHop;

    private final int maxHop;

    public SqlMatchEdge(SqlParserPos pos, SqlIdentifier name,
                        SqlNodeList labels, SqlNode where,
                        EdgeDirection direction,
                        int minHop, int maxHop) {
        super(pos, name, labels, where);
        this.direction = direction;
        this.minHop = minHop;
        this.maxHop = maxHop;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlMatchEdgeOperator.INSTANCE;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_EDGE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getName() == null && getLabels() == null && getWhere() == null) {
            switch (direction) {
                case IN:
                    writer.print("<-");
                    break;
                case OUT:
                    writer.print("->");
                    break;
                case BOTH:
                    writer.print("-");
                    break;
                default:
                    throw new IllegalArgumentException("Illegal direction: " + direction);
            }
        } else {
            if (direction == EdgeDirection.IN) {
                writer.print("<");
            }
            writer.print("-[");
            unparseNode(writer);
            writer.print("]-");
            if (direction == EdgeDirection.OUT) {
                writer.print(">");
            }
            if (minHop != -1 || maxHop != -1) {
                writer.print("{");
                if (minHop != -1) {
                    writer.print(minHop);
                }
                writer.print(",");
                if (maxHop != -1) {
                    writer.print(maxHop);
                }
                writer.print("}");
            }
        }

    }

    public EdgeDirection getDirection() {
        return direction;
    }

    public enum EdgeDirection {
        OUT,
        IN,
        BOTH;

        public static EdgeDirection of(String value) {
            for (EdgeDirection direction : EdgeDirection.values()) {
                if (direction.name().equalsIgnoreCase(value)) {
                    return direction;
                }
            }
            throw new IllegalArgumentException("Illegal direction value: " + value);
        }

        public static EdgeDirection reverse(EdgeDirection direction) {
            return direction == BOTH ? direction : ((direction == IN) ? OUT : IN);
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    public int getMinHop() {
        return minHop;
    }

    public int getMaxHop() {
        return maxHop;
    }

    public boolean isRegexMatch() {
        return minHop != 1 || maxHop != 1;
    }
}
