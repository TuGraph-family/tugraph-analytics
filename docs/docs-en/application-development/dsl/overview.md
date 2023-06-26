# Hybrid-DSL Introduction
Hybrid-DSL is a data analysis language provided by GeaFlow, which supports standard SQL+ISO/GQL for analysis on graph and tables. Through Hybrid-DSL, relational operations can be performed on table data, and graph matching and graph algorithm calculation can be performed on graph data. It also supports processing table and graph data at the same time.

# Hybrid-DSL Cases

- **Process GQL return results through SQL**

```sql
    SELECT
    a.id,
    b.id,
    AVG(e.amt),
    MAX(e.amt)
  
    FROM (
    MATCH (a) -[e:knows]->(b:person where b.id != 1)
    RETURN a, e, b
    ) 
    Group By a.id, b.id
    Having AVG(e.amt) > 10
```

  The path returned by GQL Match can be further analyzed and processed through SQL.



- **Trigger GQL graph query through SQL**

```sql
    SELECT *
    FROM (
      WITH p AS (
    	SELECT * FROM (VALUES(1, 'r0', 0.4), (4, 'r1', 0.5)) AS t(id, name, weight)
      )
      MATCH (a:person where id = p.id) -[e where weight > p.weight]->(b)
      RETURN p.name as name, a.id as a_id, e.weight as weight, b.id as b_id
    )
```

  It is possible to define a parameter table for GQL, where the data in the parameter table triggers GQL queries one by one. GQL will return the computation results corresponding to each parameter separately.

# Maven依赖
* Developing UDF/UDAF/UDTF/UDGA requires adding the following dependencies:

```xml
 <dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-dsl-common</artifactId>
    <version>0.1</version>
</dependency>
```

* To develop a custom Connector, add the following dependencies:

```xml
<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-dsl-connector-api</artifactId>
    <version>0.1</version>
</dependency>
```

# DSL Syntax Documents
* DSL Syntax
    * [DDL](reference/ddl.md)
    * [DML](reference/dml.md)
    * DQL
        * [Select](reference/dql/select.md)
        * [Union](reference/dql/union.md)
        * [Match](reference/dql/match.md)
        * [With](reference/dql/with.md)
    * [USE](reference/use.md)
* Build-in Functions
    * [Math Operation](build-in/math.md)
    * [Logical Operation](build-in/logical.md)
    * [String Function](build-in/string.md)
    * [Date Function](build-in/date.md)
    * [Condition Function](build-in/condition.md)
    * [Aggregate Function](build-in/aggregate.md)
    * [Table Function](build-in/table.md)
* User Defined Functions
    * [UDF](udf/udf.md)
    * [UDTF](udf/udtf.md)
    * [UDAF](udf/udaf.md)
    * [UDGA](udf/udga.md)
* Connector
    * [Hive Connector](connector/hive.md)
    * [File Connector](connector/file.md)
    * [Kafka Connector](connector/kafka.md)
    * [User Defined Connector](connector/udc.md)
   