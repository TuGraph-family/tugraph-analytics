package com.antgroup.geaflow.console.core.service.llm;

import com.antgroup.geaflow.console.common.util.VelocityUtil;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowEdge;
import com.antgroup.geaflow.console.core.model.data.GeaflowEndpoint;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

public class GraphSchemaTranslator {

    private static final String VERTEX_TEMPLATE = "template/graphSchema.vm";

    private static final String END_FLAG = "END_FLAG";


    public static String translateGraphSchema(GeaflowGraph graph) {
        HashMap<String, Object> velocityMap = new HashMap<>();

        Map<String, GeaflowVertex> vertexMap = graph.getVertices().values().stream().collect(Collectors.toMap(GeaflowId::getId, e -> e));
        Map<String, GeaflowEdge> edgeMap = graph.getEdges().values().stream().collect(Collectors.toMap(GeaflowId::getId, e -> e));

        Map<String, NameEndpoint> endpointMap = new HashMap<>();
        // format endpoints
        for (GeaflowEndpoint endpoint : graph.getEndpoints()) {
            GeaflowEdge edge = edgeMap.get(endpoint.getEdgeId());
            GeaflowVertex source = vertexMap.get(endpoint.getSourceId());
            GeaflowVertex target = vertexMap.get(endpoint.getTargetId());
            if (edge != null && source != null && target != null) {
                endpointMap.put(edge.getName(), new NameEndpoint(source.getName(), target.getName()));
            }
        }

        velocityMap.put("vertices", graph.getVertices().values());
        velocityMap.put("edges", graph.getEdges().values());
        velocityMap.put("endpoints", endpointMap);
        velocityMap.put("endFlag", END_FLAG);
        String s = VelocityUtil.applyResource(VERTEX_TEMPLATE, velocityMap);
        return StringUtils.replacePattern(s, ",\n*\\s*" + END_FLAG, "");
    }


    @AllArgsConstructor
    @Setter
    @Getter
    public static class NameEndpoint {

        String sourceName;
        String targetName;
    }
}
