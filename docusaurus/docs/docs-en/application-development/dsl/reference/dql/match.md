# Match
* [Match](#Match)
* [Regex-Match](#Regex-Match)
* [Return](#Return)
* [Let](#Let)
* [SubQuery](#SubQuery)
* [Continue-Match](#Continue-Match)

## Syntax

```sql
MatchStatement: MATCH PathPatthern (',' PathPatthern)* [WHERE boolExpr]

PathPatthern: Node ([Edge] Node)*
Node: '(' Identifier [ ':' StringLiteral ] [ WHERE boolExpr]
Edge: '-' '[' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ']' '-'
		 | '-' '[' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ']' '->'
		 | '<-' '[' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ']' '-'
```
### Node
Match a vertex in the graph.
### Edge
Match an edge in the graph. You can defined three kinds of edge direction: **In**, **Out** and **Both**.
### Edge Direction

| In Edge | Out Edge | Both Edge |
| -------- | -------- | -------- |
| <-[edge]- | -[edge]->     | -[edge]-   |

## Example

* Basic Mathch
```sql
-- Match all node 
MATCH (a)

-- Match all person node
MATCH (a:person)

-- Match node where id = 1
MATCH (a:person where id = 1)

-- One hop match
MATCH (a:person where id = 1)-[e:knows where e.weight > 0.4]->(b:person)

-- Tow hop match
MATCH (a:person)-(b:person) <- (c)

-- Match in-vertex for node a
MATCH (a:person)<-[e:knows]-(b)
```
* Match With Filter

```sql
MATCH (a:person)<-[e:knows]-(b) Where a.id = b.id

```
* Match Join
  Match two path pattern and join them with the common label.
  e.g.

```sql
MATCH (a) -> (b), (a) -> (c)
```
The output is **p1 = (a, b) join p2 = (a, c) on p1.a = p2.a**.

# Regex-Match
## Syntax

```sql
PathPatthern: Node Edge '{' minHop ',' [ maxHop] '}' Node
```
## Example

```sql
MATCH (a) -[e]->{1,5} (b)

MATCH (a) -[e]->{1,}  (b)
```
# Return
## Syntax
```sql
RETURN expr {',' expr}* 
[ GROUP BY expr {',' expr}* ] 
[ ORDER BY expr [ASC|DESC] {',' expr [ASC|DESC]} ]
[ LIMIT number ]
```
## Example

```sql
MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN a.name as name, b.id as b_id

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN a, b

-- GROUP BY
MATCH (a:person)-[e:knows where e.weight > 0.4]->(b:person)
RETURN a.id, SUM(e.weight) * 10 as amt GROUP BY a.id

-- ORDER BY
MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN a, b order by a.age DESC, b.age ASC

-- LIMIT
MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN a, b order by a.age DESC, b.age ASC LIMIT 10
```
# Let
Let statement is used to modify the attribute for the vertex or edge on the path.
## Syntax

```sql
LET Identifier '.' Identifier = expr
```
## Example

```sql
 MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
 LET a.weight = a.age / cast(100.0 as double),
 LET b.weight = b.age / cast(100.0 as double)


MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
LET a.weight = a.age / cast(100.0 as double),
LET a.weight = a.weight * 2,
LET b.weight = 1.0,
LET b.age = 20

```
# SubQuery
## Syntax
### Scalar Query

```sql
AggregateFunction '(' PathPatthern '=>' expr ')'
```
### Exists Query

```sql
EXISTS PathPatthern
```
## Example
### Scalar Query Example

```sql
MATCH (a:person WHERE id = 1)-[e]->(b)
Where COUNT((b) ->(c) => c) >= 1
RETURN a, e, b

MATCH (a:person WHERE id = 1)-[e]->(b)
Let b.out_cnt = COUNT((b) ->(c) => c),
Let b.out_weight = SUM((b) -[e1]-> (c) => e1.weight)
RETURN a, e, b
```
### Exists Query Example

```sql
MATCH (a:person WHERE id = 1)-[e]->(b)
Where EXISTS (b) -> (c)
      And SUM((b) -[e1]-> (c) => e1.weight) > 1
RETURN a, e, b
```

# Continue-Match
You can write a match follow another match. The return path will be the join of the two match.
## Syntax

```sql
MatchStatement
MatchStatement
```
## Example

```sql
MATCH (a:person where a.id = 1) -[e:knows]->(b:person)
LET a.weight = a.age / cast(100.0 as double),
LET a.weight = a.weight * 2
MATCH(b) -[]->(c)
RETURN a.id as a_id, a.weight as a_weight, b.id as b_id, c.id as c_id


MATCH (a) -> (b) where b.id > 0 and a.lang is null
MATCH (a) <- (c) where label(c) = 'person'
Let c.kind = 'k' || cast(c.age / 10 as varchar)
MATCH (c) -> (d) where d != b
Let d.type = if (label(d) = 'person', 1, 0)
RETURN a, b, c, d
```