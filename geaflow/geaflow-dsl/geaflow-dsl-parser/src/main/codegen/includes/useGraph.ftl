SqlUseGraph SqlUseGraph():
{
 SqlIdentifier graph = null;
}
{
  <USE> <GRAPH> graph = CompoundIdentifier()
    {
        Span s = Span.of();
        return new SqlUseGraph(s.end(this), graph);
    }
}