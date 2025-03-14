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

import { IG6GraphEvent, Item } from '@antv/g6-pc';
import Graphin, { Behaviors, GraphinContextType } from '@antv/graphin';
import type { GraphinProps } from '@antv/graphin/lib/typings/type';
import React, { createContext, useContext, useEffect } from 'react';
export const GraphCanvasContextInitValue = {
  graph: null as any,
  apis: null as any,
  theme: null as any,
  layout: null as any,
};
export const GraphCanvasContext = createContext<GraphinContextType>(
  GraphCanvasContextInitValue
);

export const useGraphinContext = () => {
  return useContext(GraphCanvasContext);
};

interface GraphCanvasProps extends GraphinProps {
  getGraphCanvasContextValue?: (
    contextValue: GraphinContextType | null
  ) => void;
  onElementShow?: (element: Item) => void;
  isPreview?: boolean;
}
const { ActivateRelations, Hoverable } = Behaviors;

export const GraphCanvas: React.FC<GraphCanvasProps> = (props) => {
  const { getGraphCanvasContextValue, isPreview, onElementShow } = props;
  const graphinRef = React.createRef<GraphinContextType>();
  useEffect(() => {
    if (getGraphCanvasContextValue) {
      getGraphCanvasContextValue(graphinRef.current);
    }
    const graph = graphinRef.current?.graph;
    const handleClick = (e: IG6GraphEvent) => {
      const { item } = e;
      graph?.getNodes().forEach((node) => {
        graph?.clearItemStates(node);
      });
      graph?.getEdges().forEach((edge) => {
        graph.clearItemStates(edge);
        const sourceNode = edge.get('sourceNode');
        const targetNode = edge.get('targetNode');
        const isSelectedEdge = edge?.getID() === item?.getID();
        graph.setItemState(edge, 'selected', isSelectedEdge);
        graph.setItemState(sourceNode, 'active', isSelectedEdge);
        graph.setItemState(targetNode, 'active', isSelectedEdge);
      });
    };
    if (graph) {
      graph.on('edge:click', handleClick);
    }

    return () => {
      if (graph) {
        graph.off('edge:click', handleClick);
      }
    };
  }, [graphinRef]);

  return (
    <Graphin
      ref={graphinRef as any}
      containerId="graph-canvas"
      style={{
        background: '#F6F8FF',
      }}
      animate={false}
      {...props}
    >
      {props.children}
      <ActivateRelations />
      <Hoverable bindType="node" />
      <Hoverable bindType="edge" />
    </Graphin>
  );
};
