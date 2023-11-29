import request from "./request";

/* Create Node*/
export async function createNode(params: CreateNode) {
  return request(`/api/data/node`, {
    method: "POST",
    data: {
      ...params,
    },
  });
}

/* Create Edge*/
export async function createEdge(params: CreateEdge) {
  return request(`/api/data/edge`, {
    method: "POST",
    data: {
      ...params,
    },
  });
}

/* Edit Node*/
export async function editNode(params: EditNode) {
  return request(`/api/data/node`, {
    method: "PUT",
    data: {
      ...params,
    },
  });
}

/* Edit Edge*/
export async function editEdge(params: EditEdge) {
  return request(`/api/data/edge`, {
    method: "PUT",
    data: {
      ...params,
    },
  });
}

/* Delete Node*/
export async function deleteNode(params: DeleteNode) {
  return request(`/api/data/node`, {
    method: "DELETE",
    data: {
      ...params,
    },
  });
}

/* Delete Edge*/
export async function deleteEdge(params: DeleteEdge) {
  return request(`/api/data/edge`, {
    method: "DELETE",
    data: {
      ...params,
    },
  });
}
