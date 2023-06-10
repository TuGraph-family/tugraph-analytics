
SqlDrop SqlDropGraph(Span s, boolean replace) :
{
    SqlIdentifier graph = null;
}
{
    <GRAPH> graph = CompoundIdentifier()
    {
        return new SqlDropGraph(s.end(this), graph);
    }
}