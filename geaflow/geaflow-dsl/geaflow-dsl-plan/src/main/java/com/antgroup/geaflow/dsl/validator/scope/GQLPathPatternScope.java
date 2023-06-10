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

package com.antgroup.geaflow.dsl.validator.scope;

import com.antgroup.geaflow.dsl.calcite.GraphRecordType;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchNode;
import com.antgroup.geaflow.dsl.validator.GQLValidatorImpl;
import com.antgroup.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace.MatchNodeContext;
import java.util.Collection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class GQLPathPatternScope extends GQLScope {

    public GQLPathPatternScope(SqlValidatorScope parent, SqlCall pathPattern) {
        super(parent, pathPattern);
    }

    @Override
    protected boolean ignoreColumnAmbiguous() {
        return true;
    }

    /**
     * Resolve the type for {@link SqlMatchNode}.
     */
    public RelDataType resolveTypeByLabels(SqlMatchNode matchNode, MatchNodeContext context) {
        boolean isVertex = matchNode.getKind() == SqlKind.GQL_MATCH_NODE;
        Collection<String> labels = matchNode.getLabelNames();
        String nodeName = matchNode.getName();

        GraphRecordType graphType = findCurrentGraphType((GQLValidatorImpl) validator, this);
        if (graphType == null) {
            boolean caseSensitive = ((GQLValidatorImpl) getValidator()).isCaseSensitive();
            RelDataTypeField resolvedField = context.getResolvedField(nodeName, caseSensitive);
            if (resolvedField != null) {
                return resolvedField.getType();
            } else {
                throw new GeaFlowDSLException(matchNode.getParserPosition(), "Match node: {} is not find."
                    , matchNode.getName());
            }
        } else {
            if (isVertex) {
                return graphType.getVertexType(labels, validator.getTypeFactory());
            }
            return graphType.getEdgeType(labels, validator.getTypeFactory());
        }
    }

    public static GraphRecordType findCurrentGraphType(GQLValidatorImpl validator, SqlValidatorScope scope) {
        if (scope instanceof ListScope) {
            SqlValidatorNamespace namespace = ((ListScope) scope).children.get(0).getNamespace();
            return findCurrentGraphType(validator, namespace);
        }
        return null;
    }

    /**
     * Find the graph using by current scope.
     */
    public static GraphRecordType findCurrentGraphType(GQLValidatorImpl validator, SqlValidatorNamespace childNs) {
        RelDataType type = childNs.getType();
        if (type instanceof GraphRecordType) {
            GraphRecordType graphType = (GraphRecordType) type;
            GraphRecordType modifyGraphType =
                validator.getCurrentQueryNodeContext().getModifyGraph(graphType.getGraphName());
            if (modifyGraphType != null) {
                // If the graph type has been modified by "Let Global" at current query-node-level context,
                // then return the modified type.
                return modifyGraphType;
            }
            return graphType;
        }
        SqlNode inputNode = childNs.getNode();
        SqlValidatorScope inputScope = (validator).getScopes(inputNode);

        if (inputScope instanceof ListScope && ((ListScope) inputScope).getChildren().size() == 1) {
            return findCurrentGraphType(validator, ((ListScope) inputScope).getChildren().get(0));
        }
        return null;
    }
}
