SqlCall SqlDescGraph() :
{
    SqlIdentifier graph = null;
}
{
    <DESC> <GRAPH> graph = CompoundIdentifier()
    {
        Span s = Span.of();
        return new SqlDescGraph(s.end(this), graph);
    }
}