package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.DoubleType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.util.IntersectionsUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Calculate the similarity between point sets
 */
@Description(name = "jaccard", description = "Jaccard similarity")
public class JaccardAlgorithm implements AlgorithmUserFunction {

    private AlgorithmRuntimeContext context;

    private int iteration = 1;

    //Similarity truncation Limit the similarity greater than how much it is counted Filter out useless similar nodes
    private double similarityCutoff;

    @Override
    public void init(AlgorithmRuntimeContext context, Object[] params) {
        this.context = context;

        if (params.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([alpha, [convergence, [max_iteration]]])");
        }
        if (params.length > 0) {
            similarityCutoff = Double.parseDouble(String.valueOf(params[0]));
        }
        if (params.length > 1) {
            iteration = Integer.parseInt(String.valueOf(params[1]));
        }
    }

    @Override
    public void process(RowVertex vertex, Iterator messages) {

        List<RowEdge> bothEdges = new ArrayList<>(context.loadEdges(EdgeDirection.BOTH));
        StringBuilder sb = new StringBuilder();
        List<String> collection = bothEdges.stream().map(x -> String.valueOf(x.getTargetId())).collect(Collectors.toList());

        if (context.getCurrentIterationId()  == iteration) {

            bothEdges.stream().forEach(x -> {
                // Update the value of the current iteration table
                context.updateVertexValue(ObjectRow.create(x.getSrcId(), x.getTargetId(), 0));
            });

            sendMessageToNeighbors(bothEdges, ObjectRow.create(vertex.getId(), collection.toString()));
        } else  {
            while (messages.hasNext()) {
                ObjectRow singleRow = (ObjectRow) messages.next();
                // set of neighbors
                Long neighborsVertexId =  (Long) singleRow.getField(0, LongType.INSTANCE);
                String vertexCollection = (String) singleRow.getField(1, StringType.INSTANCE);
                vertexCollection = vertexCollection.substring(1, vertexCollection.length()-1);
                if (StringUtils.isBlank(vertexCollection)) {
                    return;
                } else {
                    vertexCollection = vertexCollection.substring(1, vertexCollection.length()-1);
                    String[] vertexIds =  vertexCollection.split(",");
                    List<Long> neighborsVertexCollection =  Arrays.asList(vertexIds).stream().map(Long::valueOf).collect(Collectors.toList());
                    List<Long> currentCollection = collection.stream().map(Long::valueOf).collect(Collectors.toList());
                    Long[] collection1 = neighborsVertexCollection.toArray(new Long[neighborsVertexCollection.size()]);
                    Long[] collection2 = currentCollection.toArray(new Long[currentCollection.size()]);
                    double similar =  computeSimilar(collection1, collection2);
                    context.updateVertexValue(ObjectRow.create(neighborsVertexId, vertex.getId(), similar));
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex) {
        long srcId = (long) vertex.getValue().getField(0, LongType.INSTANCE);
        long targetId =  (long) vertex.getValue().getField(1, LongType.INSTANCE);
        double similar =  (double) vertex.getValue().getField(2, DoubleType.INSTANCE);
        context.take(ObjectRow.create(srcId, targetId, similar));
    }

    @Override
    public StructType getOutputType() {
        return new StructType(
            new TableField("currentid", IntegerType.INSTANCE, false),
            new TableField("neighborid", IntegerType.INSTANCE, false),
            new TableField("correlation", DoubleType.INSTANCE, false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> outEdges, Object message) {
        for (RowEdge rowEdge : outEdges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }

    public double computeSimilar(Long[] vector1, Long[] vector2) {
        long intersection = IntersectionsUtil.intersection(vector1, vector2);
        long union = vector1.length + vector2.length - intersection;
        double similarity = union == 0 ? 0 : intersection / (double) union;
        return similarity >= similarityCutoff ? similarity : Double.NaN;

    }

}
