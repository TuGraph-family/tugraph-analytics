SqlCall SqlAlterGraph():
{
    final SqlIdentifier id;
    SqlNode vertex;
    final List<SqlNode> vertices = new ArrayList<SqlNode>();
    SqlNode edge;
    final List<SqlNode> edges = new ArrayList<SqlNode>();
}
{
    <ALTER> <GRAPH> id = CompoundIdentifier()
    <ADD>
    (
        ( vertex = GraphVertex() { vertices.add(vertex);})
        |
        ( edge = GraphEdge() { edges.add(edge); } )
    ) {
        Span s = Span.of();
        return new SqlAlterGraph(
                        s.end(this),
                        id,
                        new SqlNodeList(vertices, s.addAll(vertices).pos()),
                        new SqlNodeList(edges, s.addAll(edges).pos()));
      }
}