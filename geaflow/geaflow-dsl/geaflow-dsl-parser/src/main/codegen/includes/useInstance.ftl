SqlUseInstance SqlUseInstance():
{
 SqlIdentifier instance = null;
}
{
  <USE> <INSTANCE> instance = CompoundIdentifier()
    {
        Span s = Span.of();
        return new SqlUseInstance(s.end(this), instance);
    }
}