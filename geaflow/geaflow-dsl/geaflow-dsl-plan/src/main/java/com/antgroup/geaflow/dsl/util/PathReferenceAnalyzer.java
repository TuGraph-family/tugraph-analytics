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

package com.antgroup.geaflow.dsl.util;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.LoopUntilMatch;
import com.antgroup.geaflow.dsl.rel.match.MatchJoin;
import com.antgroup.geaflow.dsl.rel.match.SingleMatchNode;
import com.antgroup.geaflow.dsl.rex.PathInputRef;
import com.antgroup.geaflow.dsl.rex.RexLambdaCall;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeName;

public class PathReferenceAnalyzer {

    private final GQLContext gqlContext;

    /**
     * Mapping of the RelNode to referred path field names after the RelNode(including this node).
     */
    private final Map<RelNode, Set<String>> node2RefPathFields = new HashMap<>();

    /**
     * The next node collection for the RelNode.
     */
    private final Map<RelNode, List<RelNode>> subsequentNodes = new HashMap<>();

    public PathReferenceAnalyzer(GQLContext gqlContext) {
        this.gqlContext = gqlContext;
    }

    public RelNode analyze(RelNode node) {
        analyzePathRef(node, new HashSet<>());
        return pruneAndAdjustPathInputRef(node);
    }

    private void analyzePathRef(RelNode node, Set<String> subsequentNodeRefPathFields) {
        Set<String> refPathFields = new HashSet<>();
        if (node instanceof GraphMatch) {
            GraphMatch match = (GraphMatch) node;
            RelNode pathPattern = match.getPathPattern();
            analyzePathRef(pathPattern, subsequentNodeRefPathFields);
            // use graph match as the subsequent node for path pattern.
            subsequentNodes.put(pathPattern, Lists.newArrayList(node));

            refPathFields.addAll(subsequentNodeRefPathFields);
        } else if (node instanceof LoopUntilMatch) {
            LoopUntilMatch loopUtil = (LoopUntilMatch) node;
            PathReferenceCollector referenceCollector = new PathReferenceCollector(loopUtil.getUtilCondition());
            refPathFields.addAll(referenceCollector.getRefPathFields());
            refPathFields.addAll(subsequentNodeRefPathFields);
            // analyze loop-body
            analyzePathRef(loopUtil.getLoopBody(), refPathFields);
            List<RelNode> subNodes = subsequentNodes.get(node);
            subsequentNodes.put(loopUtil, subNodes);
            node2RefPathFields.put(node, refPathFields);
        } else if (node instanceof MatchJoin) {
            // analyze referred path fields by this node.
            PathReferenceCollector referenceCollector = new PathReferenceCollector(node);
            Set<String> joinRefPathFields = referenceCollector.getRefPathFields();
            //Find the new fields created by join, and add the related field names to reference
            List<String> joinCreatedFields = joinRefPathFields.stream().filter(
                f -> !((MatchJoin)node).getLeft().getRowType().getFieldNames().contains(f)
                    && !((MatchJoin)node).getRight().getRowType().getFieldNames().contains(f)
            ).collect(Collectors.toList());
            for (String createdField : joinCreatedFields) {
                List<String> relatedFields = joinRefPathFields.stream().filter(
                    f -> !f.equals(createdField) && createdField.indexOf(f) == 0).collect(Collectors.toList());
                if (relatedFields.size() > 0) {
                    String nameBase = relatedFields.get(0);
                    for (String related : relatedFields) {
                        nameBase = related.length() > nameBase.length() ? related : nameBase;
                    }
                    assert Integer.valueOf(createdField.substring(nameBase.length())) >= 0;
                    for (int j = 0;; j++) {
                        String name = nameBase + j;
                        if (name.equals(createdField)) {
                            break;
                        }
                        if (!joinRefPathFields.contains(name)) {
                            joinRefPathFields.add(name);
                        }
                    }
                }

            }
            refPathFields.addAll(joinRefPathFields);
            // add referred path fields by the subsequent node.
            refPathFields.addAll(subsequentNodeRefPathFields);
        } else {
            // analyze referred path fields by this node.
            PathReferenceCollector referenceCollector = new PathReferenceCollector(node);
            refPathFields.addAll(referenceCollector.getRefPathFields());
            // add referred path fields by the subsequent node.
            refPathFields.addAll(subsequentNodeRefPathFields);
        }

        node2RefPathFields.computeIfAbsent(node, n -> new HashSet<>()).addAll(refPathFields);

        for (RelNode input : node.getInputs()) {
            subsequentNodes.computeIfAbsent(input, k -> new ArrayList<>()).add(node);
            Set<String> inputSubsequent;
            if (!(node instanceof IMatchNode) && input.getRowType().getSqlTypeName() != SqlTypeName.PATH) {
                // If input's type is not a path, then it breaks the continuous match.
                // It only can be another match, so clean the subsequentNodeRefPathFields set.
                inputSubsequent = new HashSet<>();
            } else {
                inputSubsequent = refPathFields;
            }
            analyzePathRef(input, inputSubsequent);
        }
    }

    /**
     * Prune the path schema and adjust the PathInputRef index for {@link RelNode}.
     *
     * @param node The node to be pruned.
     * @return The pruned node.
     */
    private RelNode pruneAndAdjustPathInputRef(final RelNode node) {
        List<RelNode> rewriteInputs = new ArrayList<>();
        //step1. rewrite all the inputs.
        for (RelNode input : node.getInputs()) {
            rewriteInputs.add(pruneAndAdjustPathInputRef(input));
        }
        RelNode rewriteNode = node;

        //step2. adjust the index of the PathInputRef after the inputs has pruned.
        if (rewriteNode instanceof LoopUntilMatch) { // Adjust loop-util
            LoopUntilMatch loopUtil = (LoopUntilMatch) rewriteNode;
            adjustPathRefIndex(loopUtil.getUtilCondition(), getPathType(rewriteInputs.get(0)));
            pruneAndAdjustPathInputRef(loopUtil.getLoopBody());
        } else if (rewriteNode instanceof BiRel) { // Adjust for join & correlate
            rewriteNode = rewriteNode.copy(node.getTraitSet(), rewriteInputs);
            PathRecordType pathType = getPathType(rewriteNode);
            if (pathType != null) {
                // rewrite the on condition using the latest join output type.
                rewriteNode = adjustPathRefIndex(rewriteNode, pathType);
            }
        } else if (rewriteInputs.size() == 1
            && getPathType(rewriteInputs.get(0)) != null) {
            RelNode rewriteInput = rewriteInputs.get(0);
            PathRecordType inputPathType = getPathType(rewriteInput);
            rewriteNode = adjustPathRefIndex(rewriteNode, inputPathType);
            // replace input after adjust path ref index.
            rewriteNode = rewriteNode.copy(node.getTraitSet(), rewriteInputs);
        } else {
            rewriteNode = rewriteNode.copy(node.getTraitSet(), rewriteInputs);
        }

        //step3. prune path type for single match node.
        Set<String> subsequentRefFields = getSubsequentNodeRefPathFields(node);
        if (node instanceof SingleMatchNode) {
            rewriteNode = pruneMatchNode((SingleMatchNode) rewriteNode, subsequentRefFields);
        } else if (node instanceof GraphMatch) {
            // prune match node in graph match.
            GraphMatch match = (GraphMatch) node;
            IMatchNode rewritePathPattern = (IMatchNode) pruneAndAdjustPathInputRef(match.getPathPattern());
            rewriteNode = match.copy(match.getTraitSet(), rewriteInputs.get(0), rewritePathPattern,
                rewritePathPattern.getPathSchema());
        }
        return rewriteNode;
    }

    private RelNode adjustPathRefIndex(RelNode node, PathRecordType inputPathType) {
        return node.accept(new AdjustPathRefIndexVisitor(inputPathType));
    }

    private RexNode adjustPathRefIndex(RexNode node, PathRecordType inputPathType) {
        assert inputPathType != null;
        return node.accept(new AdjustPathRefIndexVisitor(inputPathType));
    }

    private class AdjustPathRefIndexVisitor extends RexShuttle {

        private final PathRecordType inputPathType;

        public AdjustPathRefIndexVisitor(PathRecordType inputPathType) {
            this.inputPathType = inputPathType;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (inputRef instanceof PathInputRef) {
                PathInputRef pathInputRef = (PathInputRef) inputRef;
                RelDataTypeField field = inputPathType.getField(pathInputRef.getLabel(),
                    gqlContext.isCaseSensitive(), false);
                assert field != null : "Field: " + pathInputRef.getLabel()
                    + " not found in the input";
                return pathInputRef.copy(field.getIndex());
            }
            return inputRef;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (call instanceof RexLambdaCall) {
                RexLambdaCall lambdaCall = (RexLambdaCall) call;
                RexSubQuery subQuery = lambdaCall.getInput();
                RexNode valueNode = lambdaCall.getValue();

                // prune sub query
                PathReferenceCollector referenceCollector = new PathReferenceCollector(valueNode);
                Set<String> refPathFields = new HashSet<>(referenceCollector.getRefPathFields());

                assert inputPathType.lastFieldName().isPresent();
                // The last field is the start vertex to request the sub query.
                String startLabel = inputPathType.lastFieldName().get();
                refPathFields.add(startLabel);
                analyzePathRef(subQuery.rel, refPathFields);
                // In order to getSubsequentNodeRefPathFields for subQuery.rel when pruning it,
                // we attach subQuery.rel to itself as it has no real next node.
                subsequentNodes.put(subQuery.rel, Lists.newArrayList(subQuery.rel));
                // prune sub query
                RelNode newSubRel = pruneAndAdjustPathInputRef(subQuery.rel);
                RexSubQuery newSubQuery = subQuery.clone(newSubRel);
                // adjust path index for value node
                RexNode newValue = adjustPathRefIndex(valueNode, getPathType(newSubRel));
                return lambdaCall.clone(lambdaCall.type, Lists.newArrayList(newSubQuery, newValue));
            }
            return super.visitCall(call);
        }
    }

    /**
     * Prune path type for match node.
     *
     * @param node The match node.
     * @param subsequentRefFields The reference labels by the subsequent nodes.
     */
    private SingleMatchNode pruneMatchNode(SingleMatchNode node, Set<String> subsequentRefFields) {
        PathRecordType outputPathType = prunePathType(subsequentRefFields, node.getPathSchema());
        return (SingleMatchNode) node.copy(node.getInputs(), outputPathType);
    }

    private PathRecordType prunePathType(Set<String> refPathFields, RelDataType pathRecordType) {
        // Pruned the path type by the reference.
        List<RelDataTypeField> prunedFields = new ArrayList<>();
        int index = 0;
        for (RelDataTypeField field : pathRecordType.getFieldList()) {
            if (refPathFields.contains(field.getName())) {
                prunedFields.add(new RelDataTypeFieldImpl(field.getName(), index, field.getType()));
                index++;
            }
        }
        return new PathRecordType(prunedFields);
    }

    private Set<String> getRefPathFields(RelNode node) {
        return node2RefPathFields.get(node);
    }

    private Set<String> getSubsequentNodeRefPathFields(RelNode node) {
        List<RelNode> subNodes = subsequentNodes.get(node);
        if (subNodes != null && subNodes.size() > 0) {
            return subNodes.stream().map(this::getRefPathFields).reduce(Sets::union).get();
        }
        return new HashSet<>();
    }

    private static PathRecordType getPathType(RelNode node) {
        if (node instanceof IMatchNode) {
            return ((IMatchNode) node).getPathSchema();
        }
        if (node.getRowType() instanceof PathRecordType) {
            return (PathRecordType) node.getRowType();
        }
        return null;
    }

    private static class PathReferenceCollector extends RexShuttle {

        /**
         * The RelNode to collect referred path fields.
         */
        private RelNode node;

        private RexNode rexNode;

        private final Set<String> refPathFields = new HashSet<>();

        private boolean hasAnalyze = false;

        public PathReferenceCollector(RelNode node) {
            this.node = node;
        }

        public PathReferenceCollector(RexNode rexNode) {
            this.rexNode = rexNode;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (inputRef instanceof PathInputRef) {
                refPathFields.add(((PathInputRef) inputRef).getLabel());
            }
            return inputRef;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (call instanceof RexLambdaCall) {
                // analyze path reference in sub query.
                assert node != null : "node should not be null when analyze sub query.";
                RelDataType inputPathType;
                if (node instanceof SingleMatchNode) {
                    inputPathType = ((SingleMatchNode) node).getInput().getRowType();
                } else if (node instanceof MatchJoin) {
                    inputPathType = node.getRowType();
                } else {
                    throw new IllegalArgumentException("Illegal node: " + node + " with sub-query");
                }
                assert inputPathType != null;

                int parentPathSize = inputPathType.getFieldCount();
                RexLambdaCall lambdaCall = (RexLambdaCall) call;
                RexSubQuery subQuery = lambdaCall.getInput();
                RelDataType subQueryPathType = subQuery.rel.getRowType();
                // The first node of the sub query is the start vertex, it cannot be pruned.
                refPathFields.add(subQueryPathType.getFieldList().get(parentPathSize - 1).getName());

                PathReferenceCollector subCollector = new PathReferenceCollector(subQuery.rel);
                Set<String> subRefPathFields = subCollector.getRefPathFields();
                inputPathType.getFieldNames().stream()
                    .filter(subRefPathFields::contains)
                    .forEach(refPathFields::add);
            }
            return super.visitCall(call);
        }

        public Set<String> getRefPathFields() {
            if (!hasAnalyze) {
                if (node != null) {
                    node.accept(this);
                } else if (rexNode != null) {
                    rexNode.accept(this);
                }
                hasAnalyze = true;
            }
            return refPathFields;
        }
    }
}
