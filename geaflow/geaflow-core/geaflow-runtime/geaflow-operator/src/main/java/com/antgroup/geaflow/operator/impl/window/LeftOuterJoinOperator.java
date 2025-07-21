// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing,
//  * software distributed under the License is distributed on an
//  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  * KIND, either express or implied.  See the License for the
//  * specific language governing permissions and limitations
//  * under the License.
//  */

// package com.antgroup.geaflow.operator.impl.window;

// import com.antgroup.geaflow.api.function.base.JoinFunction;

// import com.antgroup.geaflow.api.function.base.KeySelector;

// import com.antgroup.geaflow.operator.base.AbstractTwoInputOperator;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;

// public class LeftOuterJoinOperator<L, R, K, O> extends AbstractTwoInputOperator<L, R, O> {

//     private final KeySelector<L, K> leftKeySelector;
//     private final KeySelector<R, K> rightKeySelector;
//     private final JoinFunction<L, R, O> joinFunction;

//     // 用于缓存当前窗口数据的状态
//     private Map<K, List<L>> leftWindowData;
//     private Map<K, List<R>> rightWindowData;

//     public LeftOuterJoinOperator(KeySelector<L, K> leftKeySelector,
//             KeySelector<R, K> rightKeySelector,
//             JoinFunction<L, R, O> joinFunction) {
//         this.leftKeySelector = leftKeySelector;
//         this.rightKeySelector = rightKeySelector;
//         this.joinFunction = joinFunction;
//     }

//     @Override
//     public void open(OpContext opContext) {
//         super.open(opContext);
//         this.leftWindowData = new HashMap<>();
//         this.rightWindowData = new HashMap<>();
//     }

//     @Override
//     protected void processLeft(L value) throws Exception {
//         K key = this.leftKeySelector.getKey(value); // getKey() 方法在 KeySelector 中同样存在
//         this.leftWindowData.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
//     }

//     @Override
//     protected void processRight(R value) throws Exception {
//         K key = this.rightKeySelector.getKey(value);
//         this.rightWindowData.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
//     }

//     @Override
//     public void finish() throws Exception {
//         // 遍历左表的所有数据
//         for (Map.Entry<K, List<L>> entry : this.leftWindowData.entrySet()) {
//             K key = entry.getKey();
//             List<L> leftValues = entry.getValue();
//             List<R> rightValues = this.rightWindowData.get(key);

//             if (rightValues != null && !rightValues.isEmpty()) {
//                 // Case 1: 右边能找到匹配的 Key
//                 for (L leftValue : leftValues) {
//                     for (R rightValue : rightValues) {
//                         this.collector.collect(this.joinFunction.join(leftValue, rightValue));
//                     }
//                 }
//             } else {
//                 // Case 2: 右边找不到匹配的 Key，左连接的核心逻辑
//                 for (L leftValue : leftValues) {
//                     this.collector.collect(this.joinFunction.join(leftValue, null));
//                 }
//             }
//         }
//         // 清空当前窗口的缓存
//         this.leftWindowData.clear();
//         this.rightWindowData.clear();
//     }
// }