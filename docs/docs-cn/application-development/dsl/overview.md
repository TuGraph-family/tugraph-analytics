# 融合DSL概述
融合DSL是GeaFlow提供的图表一体的数据分析语言，支持标准SQL+ISO/GQL进行图表分析.通过融合DSL可以对表数据做关系运算处理，也可以对图数据做图匹配和图算法计算，同时也支持同时图表数据的联合处理.

# 融合DSL使用案例

- **通过SQL处理GQL结果**

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

  GQL Match返回的Path可以通过SQL做进一步分析处理.



- **通过SQL触发GQL图查询**

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

  可以给GQL定义一个参数表，参数表的数据逐条触发GQL查询.GQL将分别返回每个参数对应的计算结果.

# Maven依赖
* 开发UDF/UDAF/UDTF/UDGA需要添加以下依赖：

```xml
 <dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-dsl-common</artifactId>
    <version>0.1</version>
</dependency>
```
* 开发自定义Connector需添加以下依赖：

```xml
<dependency>
    <groupId>com.antgroup.tugraph</groupId>
    <artifactId>geaflow-dsl-connector-api</artifactId>
    <version>0.1</version>
</dependency>
```

# DSL语法文档
* DSL语法
  * [DDL](reference/ddl.md)
  * [DML](reference/dml.md)
  * DQL
    * [Select](reference/dql/select.md)
    * [Union](reference/dql/union.md)
    * [Match](reference/dql/match.md)
    * [With](reference/dql/with.md)
  * [USE](reference/use.md)
* 内置函数
  * [数学运算](build-in/math.md)
  * [逻辑运算](build-in/logical.md)
  * [字符串函数](build-in/string.md)
  * [日期函数](build-in/date.md)
  * [条件函数](build-in/condition.md)
  * [聚合函数](build-in/aggregate.md)
  * [表处理函数](build-in/table.md)
* 用户自定义函数
  * [UDF](udf/udf.md)
  * [UDTF](udf/udtf.md)
  * [UDAF](udf/udaf.md)
  * [UDGA](udf/udga.md)
* Connector
  * [Hive Connector](connector/hive.md)
  * [File Connector](connector/file.md)
  * [Kafka Connector](connector/kafka.md)
  * [用户自定义Connector](connector/udc.md)

   