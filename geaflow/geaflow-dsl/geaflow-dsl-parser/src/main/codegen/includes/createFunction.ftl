
SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    SqlNode functionName = null;
    SqlNode className = null;
    SqlNode usingPath = null;
    boolean ifNotExists = false;
}
{
    <FUNCTION> [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]

    functionName = CompoundIdentifier()

    <AS>  className = StringLiteral()

    [<USING> usingPath = StringLiteral()]
    {
        return new SqlCreateFunction(s.end(this), ifNotExists, functionName, className, usingPath);
    }
}