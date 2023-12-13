import { find } from 'lodash';
export const editEdgeParamsTransform = (sourceModel, targetModel, nodes) => {
  const sourceLabel = sourceModel?.label;
  const targetLabel = targetModel?.label;
  const sourceNode = find(nodes, (node) => node.labelName === sourceLabel);
  const targetNode = find(nodes, (node) => node.labelName === targetLabel);
  const sourcePrimaryKey = sourceNode?.primaryField;
  const targetPrimaryKey = targetNode?.primaryField;
  const sourceValue = sourceModel.properties[sourcePrimaryKey];
  const targetValue = targetModel.properties[targetPrimaryKey];
  return { sourceLabel, targetLabel, sourcePrimaryKey, targetPrimaryKey, sourceValue, targetValue };
};
