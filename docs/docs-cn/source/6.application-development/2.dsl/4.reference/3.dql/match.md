# Match
GeaFlow支持以下语法 ：
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
Node: '(' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ')'
Edge: '-' '[' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ']' '-'
		 | '-' '[' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ']' '->'
		 | '<-' '[' Identifier [ ':' StringLiteral ] [ WHERE boolExpr] ']' '-'
```
### Node
匹配图上的点,可以指定点的类型以及对点的过滤条件。
### Edge
匹配图上的边，类似Node节点可以指定边的类型以及对边的过滤条件。和Node不同的是,边需要指定方向，边的方向包括入边、出边和双向边。
### Edge Direction

| 边类型 | 语法 | 说明 |
| -------- | -------- | -------- |
| 入边 |  <-[edge]- | 匹配图中指向点的边|
| 出边 | -[edge]-> | 匹配图中从点指出的边 |
| 双向边 | -[edge]- | 匹配图中点的出边和入边|

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
不定跳数匹配，类似正则表达式写法指定跳数。
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
Let语句用于在图匹配过程中修改路径上的点和边的属性值。
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
用户可以将一段复杂的Match拆分成多段Match，多段Match的结果为各个Match路径的join关联。
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