// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing,
//  * software distributed under the License is distributed on an
//  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  * KIND, either express or implied.  See the License for the
//  * specific language governing permissions and limitations
//  * under the License.
//  */

// package com.antgroup.geaflow.pdata.stream.window;

// import com.antgroup.geaflow.api.pdata.PStream;
// import com.antgroup.geaflow.api.transform.TransformType;
// import com.antgroup.geaflow.dsl.parser.SqlMatchPattern;

// import com.antgroup.geaflow.dsl.rel.match.IMatchNode;

// import com.antgroup.geaflow.pdata.stream.AbstractStream;

// /**
//  * A stream that represents an OPTIONAL MATCH operation on streams of Paths.
//  * It holds the left and right input streams and all necessary functions for the
//  * join.
//  */
// public class WindowOptionalMatchStream extends WindowDataStream<Path> {

//     private final IMatchNode optionalPattern;

//     private final boolean isCaseSensitive; // 新增字段

//     // 新增 isCaseSensitive 到构造函数
//     public WindowOptionalMatchStream(PStream<T> input, IMatchNode pathPattern, boolean isCaseSensitive) {
//         super(input.getContext(), ((PWindowStream<T>) input).getWindow());
//         this.parents.add((AbstractStream<T>) input);
//         this.pathPattern = pathPattern;
//         this.isCaseSensitive = isCaseSensitive;
//         System.out.println("\n--- 步骤 6/8: WindowOptionalMatchStream 实例已创建 ---\n");
//     }

//     public Stream<Path> getRightStream() {
//         return rightStream;
//     }

//     public KeySelector<Path, ?> getLeftKeySelector() {
//         return leftKeySelector;
//     }

//     public KeySelector<Path, ?> getRightKeySelector() {
//         return rightKeySelector;
//     }

//     public StepJoinFunction getJoinFunction() {
//         return joinFunction;
//     }

//     public IMatchNode getPathPattern() {
//         return pathPattern;
//     }

//     @Override
//     public TransformType getTransformType() {
//         System.out.println("\n--- 步骤 6.1/8: WindowOptionalMatchStream.getTransformType() 已被调用 ---\n");
//         return TransformType.OptionalMatch;
//     }
// }
