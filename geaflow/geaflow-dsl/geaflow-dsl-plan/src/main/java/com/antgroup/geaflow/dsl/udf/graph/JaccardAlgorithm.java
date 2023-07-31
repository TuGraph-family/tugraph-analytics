package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

@Description(name = "jaccard", description = "Jaccard similarity")
public class JaccardAlgorithm implements AlgorithmUserFunction<Object, Row> {

    private AlgorithmRuntimeContext context;
    private int iteration = 20;

    private List<String> sourceList;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Row> context, Object[] params) {
        this.context = context;
    }

    @Override
    public void process(RowVertex vertex, Iterator<Row> messages) {

        // out 出度计算
        List<RowEdge> outEdges = new ArrayList<>(context.loadEdges(EdgeDirection.OUT));

        for (RowEdge edge: outEdges) {
            if (vertex.getId().equals(edge.getSrcId())) {
                Object target =  edge.getTargetId();
                Object source = edge.getSrcId();

            }
        }

        for (RowEdge edge: outEdges)

        context.updateVertexValue(vertex.getValue());

    }

    @Override
    public StructType getOutputType() {
        return null;
    }

    private void sendMessageToNeighbors(List<RowEdge> outEdges, Object message) {
        for (RowEdge rowEdge : outEdges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }


    public static double computeJaccardsCoefficient(Row p1, Row p2) {


        Collection<String> tweet1 = new TreeSet<String>();

        Collection<String> tweet2 = new TreeSet<String>();

        Collection<String> intersectionOfTweets = new TreeSet<String>(
            tweet1);
        intersectionOfTweets.retainAll(tweet2);

        Collection<String> unionOfTweets = new TreeSet<String>(tweet1);
        unionOfTweets.addAll(tweet2);

        double jaccardsCoefficient = (double) intersectionOfTweets.size()
            / (double) unionOfTweets.size();
        return jaccardsCoefficient;
    }

}
