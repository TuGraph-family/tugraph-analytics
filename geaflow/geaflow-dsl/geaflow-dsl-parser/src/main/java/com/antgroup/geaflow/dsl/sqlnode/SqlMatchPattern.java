/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.sqlnode;

import com.antgroup.geaflow.dsl.operator.SqlMatchPatternOperator;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlMatchPattern extends SqlCall {

    private SqlNode from;

    private SqlNodeList pathPatterns;

    private SqlNode where;

    private SqlNodeList orderBy;

    private SqlNode limit;

    private final boolean isOptional;

    public SqlMatchPattern(SqlParserPos pos, SqlNode from, SqlNodeList pathPatterns,
                           SqlNode where, SqlNodeList orderBy, SqlNode limit, boolean isOptional) {
        super(pos);
        this.from = from;
        this.pathPatterns = pathPatterns;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.isOptional = isOptional;
    }

    /**
     * This constructor is retained for compatibility with code that does not yet know about the
     * isOptional flag, and defaults it to false.
     */
    public SqlMatchPattern(SqlParserPos pos, SqlNode from, SqlNodeList pathPatterns,
                           SqlNode where, SqlNodeList orderBy, SqlNode limit) {
        // Calls the new constructor, providing a default value of 'false' for isOptional.
        this(pos, from, pathPatterns, where, orderBy, limit, false);
    }


    @Override
    public SqlOperator getOperator() {
        return SqlMatchPatternOperator.INSTANCE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getFrom(), getPathPatterns(), getWhere(),
            getOrderBy(), getLimit());
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_PATTERN;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    public boolean isOptional() {
        return isOptional;
    }

    public SqlNode getFrom() {
        return from;
    }

    public void setFrom(SqlNode from) {
        this.from = from;
    }

    public SqlNodeList getOrderBy() {
        return orderBy;
    }


    public SqlNode getLimit() {
        return limit;
    }

    public void setOrderBy(SqlNodeList orderBy) {
        this.orderBy = orderBy;
    }

    public void setLimit(SqlNode limit) {
        this.limit = limit;
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.from = operand;
                break;
            case 1:
                this.pathPatterns = (SqlNodeList) operand;
                break;
            case 2:
                this.where = operand;
                break;
            case 3:
                this.orderBy = (SqlNodeList) operand;
                break;
            case 4:
                this.limit = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    /**
     * The unparse method is modified to recursively unparse the query chain.
     */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // First, recursively unparse the preceding part of the query (the 'from' chain).
        if (from != null) {
            from.unparse(writer, 0, 0);
            writer.newlineAndIndent();
        }

        // Now, unparse this specific MATCH clause.
        if (isOptional) {
            writer.keyword("OPTIONAL");
            writer.print(" ");
        }
        writer.keyword("MATCH");
        
        if (pathPatterns != null) {
            writer.print(" ");
            pathPatterns.unparse(writer, 0, 0);
        }

        if (where != null) {
            writer.newlineAndIndent();
            writer.keyword("WHERE");
            writer.print(" ");
            where.unparse(writer, 0, 0);
        }
        
        if (orderBy != null && orderBy.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("ORDER BY");
            writer.print(" ");
            orderBy.unparse(writer, leftPrec, rightPrec);
        }

        if (limit != null) {
            writer.newlineAndIndent();
            writer.keyword("LIMIT");
            writer.print(" ");
            limit.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlNodeList getPathPatterns() {
        return pathPatterns;
    }

    public SqlNode getWhere() {
        return where;
    }

    public final boolean isDistinct() {
        return false;
    }

    public void setWhere(SqlNode where) {
        this.where = where;
    }

    public boolean isSinglePattern() {
        return pathPatterns != null && pathPatterns.size() == 1 && pathPatterns.get(0) instanceof SqlPathPattern;
    }
}
