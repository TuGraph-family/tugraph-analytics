SqlVertexConstruct SqlVertexConstruct():
{
  List<SqlNode> operands = new ArrayList();
  SqlIdentifier key;
  SqlNode value;
  Span s = Span.of();
}
{
  <VERTEX> <LBRACE>
      key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
      {
         operands.add(key);
         operands.add(value);
      }
      (
        <COMMA>
        key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
           operands.add(key);
           operands.add(value);
         }
      )*
    <RBRACE>
    {
      return new SqlVertexConstruct(operands.toArray(new SqlNode[]{}), s.end(this));
    }
}


SqlEdgeConstruct SqlEdgeConstruct():
{
  List<SqlNode> operands = new ArrayList();
  SqlIdentifier key;
  SqlNode value;
  Span s = Span.of();
}
{
  <EDGE> <LBRACE>
      key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
      {
        operands.add(key);
        operands.add(value);
      }
      (
        <COMMA>
        key = SimpleIdentifier() <EQ> value = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
           operands.add(key);
           operands.add(value);
        }
      )*

      <RBRACE>
      {
        return new SqlEdgeConstruct(operands.toArray(new SqlNode[]{}), s.end(this));
      }
}

SqlPathPatternSubQuery SqlPathPatternSubQuery():
{
  SqlPathPattern pathPattern;
  SqlNode returnValue = null;
  Span s = Span.of();
}
{
  pathPattern = SqlPathPattern()
  [
    <NAMED_ARGUMENT_ASSIGNMENT> returnValue = Expression(ExprContext.ACCEPT_NON_QUERY)
  ]
  {
    return new SqlPathPatternSubQuery(pathPattern, returnValue, s.end(this));
  }
}


