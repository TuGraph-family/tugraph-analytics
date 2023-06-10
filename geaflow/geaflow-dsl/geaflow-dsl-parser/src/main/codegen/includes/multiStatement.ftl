/**
 * Parses multi SQL statement followed by the end-of-file symbol.
 */
List<SqlNode> MultiStmtEof() :
{
    List<SqlNode> multiStmt = new ArrayList();
    SqlNode stmt;
}
{
   stmt = SqlStmt() {
           multiStmt.add(stmt);
   }
   (
      <SEMICOLON>
      [
         stmt = SqlStmt() {
            multiStmt.add(stmt);
         }
      ]
    )*
    <EOF>
    {
        return multiStmt;
    }
}