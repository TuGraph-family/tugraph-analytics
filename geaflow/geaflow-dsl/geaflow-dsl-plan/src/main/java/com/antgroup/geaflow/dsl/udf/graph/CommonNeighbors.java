package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

@Description(name = "common_neighbors", description = "built-in udga for CommonNeighbors")
public class CommonNeighbors implements AlgorithmUserFunction<Object, Object> {

    private AlgorithmRuntimeContext<Object, Object> context;

    // tuple to store params
    private Tuple<Object, Object> vertices;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] params) {
        this.context = context;

        if (params.length != 2) {
            throw new IllegalArgumentException("Only support two arguments, usage: common_neighbors(id_a, id_b)");
        }
        this.vertices = new Tuple<>(
                TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType()),
                TypeCastUtil.cast(params[1], context.getGraphSchema().getIdType())
        );
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        if (context.getCurrentIterationId() == 1L) {
            // send message to neighbors if they are vertices in params
            if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), vertex.getId());
            }
        } else if (context.getCurrentIterationId() == 2L) {
            // add to result if received messages from both vertices in params
            Tuple<Boolean, Boolean> received = new Tuple<>(false, false);
            while (messages.hasNext()) {
                Object message = messages.next();
                if (vertices.f0.equals(message)) {
                    received.setF0(true);
                }
                if (vertices.f1.equals(message)) {
                    received.setF1(true);
                }

                if (received.getF0() && received.getF1()) {
                    context.take(ObjectRow.create(vertex.getId()));
                }
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("id", graphSchema.getIdType(), false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Object message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
