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

import com.antgroup.geaflow.dsl.operator.SqlPathPatternOperator;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlPathPattern extends SqlCall {

    private SqlNodeList pathNodes;

    private SqlIdentifier pathAlias;

    /**
     * The flag to identify if this path pattern is optional.
     */
    private final boolean isOptional;

    /**
     * Constructs a required path pattern (i.e., non-optional).
     * This constructor is retained for backward compatibility and is used by the
     * parser generator (e.g., in {@code GeaFlowParserImpl.java}).
     * 
     * @param pos       the position in the parsed SQL
     * @param pathNodes the list of match path nodes
     * @param pathAlias the alias for the pattern
     */
    public SqlPathPattern(SqlParserPos pos, SqlNodeList pathNodes, SqlIdentifier pathAlias) {
        this(pos, pathNodes, pathAlias, false); // 默认非 optional
    }

    /**
     * Constructs a path pattern with an optional flag.
     * This is the primary constructor used to distinguish between required and
     * optional match paths.
     * 
     * @param pos        the parser position
     * @param pathNodes  the nodes and edges in the path
     * @param pathAlias  the alias for the entire path
     * @param isOptional {@code true} if the path is from an OPTIONAL MATCH clause;
     *                   {@code false} otherwise
     */
    public SqlPathPattern(SqlParserPos pos, SqlNodeList pathNodes, SqlIdentifier pathAlias, boolean isOptional) {
        super(pos);
        this.pathNodes = Objects.requireNonNull(pathNodes);
        this.pathAlias = pathAlias;
        this.isOptional = isOptional;
    }

    /**
     * Constructs a path pattern with only the path nodes specified.
     * This constructor is primarily used by the parser or legacy code paths
     * where alias and optional match information are not specified.
     * It defaults to a required (non-optional) path without alias.
     * @param pos       The parser position in the SQL script.
     * @param pathNodes The list of nodes and edges that form the path pattern.
     */
    public SqlPathPattern(SqlParserPos pos, SqlNodeList pathNodes) {
        this(pos, pathNodes, null, false);
    }

    /**
     * Returns whether this path pattern is optional.
     * This is the key method for the converter to access the optional status.
     * 
     * @return True if the path is optional, false otherwise.
     */
    public boolean isOptional() {
        return this.isOptional;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlPathPatternOperator.INSTANCE;
    }

    /**
     * Returns the list of operands for this node.
     * We keep this method unchanged from the original to avoid compilation issues.
     * 
     * @return A list of operands.
     */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(pathNodes, pathAlias);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.pathNodes = (SqlNodeList) operand;
                break;
            case 1:
                this.pathAlias = (SqlIdentifier) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (pathAlias != null) {
            pathAlias.unparse(writer, 0, 0);
            writer.print("=");
        }
        for (SqlNode node : pathNodes) {
            node.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlNodeList getPathNodes() {
        return pathNodes;
    }

    public String getPathAliasName() {
        if (pathAlias != null) {
            return pathAlias.getSimple();
        }
        return null;
    }

    public void setPathAlias(SqlIdentifier pathAlias) {
        this.pathAlias = pathAlias;
    }

    public SqlMatchNode getFirst() {
        return (SqlMatchNode) pathNodes.get(0);
    }

    public SqlMatchNode getLast() {
        return (SqlMatchNode) pathNodes.get(pathNodes.size() - 1);
    }
}
