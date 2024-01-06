import { useRequest } from "ahooks";
import {
  createEdge,
  createNode,
  deleteEdge,
  deleteNode,
  editEdge,
  editNode,
} from "../../../../services/GraphDataController";

export const useGraphData = () => {
  const {
    runAsync: onCreateEdge,
    loading: CreateEdgeLoading,
    error: CreateEdgeError,
  } = useRequest(createEdge, { manual: true });
  const {
    runAsync: onCreateNode,
    loading: CreateNodeLoading,
    error: CreateNodeError,
  } = useRequest(createNode, { manual: true });
  const {
    runAsync: onDeleteEdge,
    loading: DeleteEdgeLoading,
    error: DeleteEdgeError,
  } = useRequest(deleteEdge, { manual: true });
  const {
    runAsync: onDeleteNode,
    loading: DeleteNodeLoading,
    error: DeleteNodeError,
  } = useRequest(deleteNode, { manual: true });
  const {
    runAsync: onEditEdge,
    loading: EditEdgeLoading,
    error: EditEdgeError,
  } = useRequest(editEdge, { manual: true });
  const {
    runAsync: onEditNode,
    loading: EditNodeLoading,
    error: EditNodeeError,
  } = useRequest(editNode, { manual: true });

  return {
    onCreateEdge,
    CreateEdgeLoading,
    CreateEdgeError,
    onCreateNode,
    CreateNodeLoading,
    CreateNodeError,
    onDeleteEdge,
    DeleteEdgeLoading,
    DeleteEdgeError,
    onDeleteNode,
    DeleteNodeLoading,
    DeleteNodeError,
    onEditEdge,
    EditEdgeLoading,
    EditEdgeError,
    onEditNode,
    EditNodeLoading,
    EditNodeeError,
  };
};
