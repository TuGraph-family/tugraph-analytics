/**
 * Parses a create view or replace existing view statement.
 *
 * CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlCreate SqlCreateView(Span s, boolean replace) :
{
    boolean ifNotExists = false;
    SqlIdentifier viewName;
    SqlNode query;
    List<SqlNode> fieldList = new ArrayList<SqlNode>();
    SqlIdentifier id;
    SqlNodeList fieldNodeList;
}
{
    <VIEW> [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    viewName = CompoundIdentifier()
    <LPAREN>
            id=SimpleIdentifier() { fieldList.add(id);  }
            (
                <COMMA> id=SimpleIdentifier()  {   fieldList.add(id);  }
            )*
            {
                fieldNodeList = new SqlNodeList(fieldList, s.addAll(fieldList).pos());
            }
    <RPAREN>
        {
            for(SqlNode node : fieldList)
            {
                if (node instanceof SqlIdentifier && ((SqlIdentifier)node).isStar())
                    throw new ParseException(String.format("View's field list has a '*', which is invalid."));
            }
            fieldNodeList = new SqlNodeList(fieldList, s.addAll(fieldList).pos());
        }
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(s.end(this), ifNotExists, viewName, fieldNodeList, query);
    }
}

/** Parses an optional field list and makes sure no field is a "*". */
SqlNodeList ParseOptionalFieldList(String relType) :
{
    SqlNodeList fieldList;
}
{
    fieldList = ParseRequiredFieldList(relType)
    {
        return fieldList;
    }
    |
    {
        return SqlNodeList.EMPTY;
    }
}

/** Parses a required field list and makes sure no field is a "*". */
SqlNodeList ParseRequiredFieldList(String relType) :
{
    List<SqlNode> fieldList = new ArrayList<SqlNode>();
    SqlIdentifier id;
    SqlNodeList fieldNodeList;
    Span s;
}
{
    <LPAREN> { s = span(); }
        id=SimpleIdentifier() { fieldList.add(id);  }
        (
            <COMMA> id=SimpleIdentifier()  {   fieldList.add(id);  }
        )*
        {
            fieldNodeList = new SqlNodeList(fieldList, s.addAll(fieldList).pos());
        }

    <RPAREN>
    {
        for(SqlNode node : fieldList)
        {
            if (node instanceof SqlIdentifier && ((SqlIdentifier)node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return new SqlNodeList(fieldList, s.end(this));
    }
}

