# 快速上手(本地运行)

## 准备工作

### 编译 GeaFlow 源码

编译 GeaFlow 依赖以下环境：

- JDK8
- Maven(推荐 3.6.3 及以上版本)
- Git

执行以下命令来编译 GeaFlow 源码：

```shell
git clone https://github.com/TuGraph-family/tugraph-analytics.git
cd tugraph-analytics/
mvn clean package -DskipTests
```

## 本地运行流图作业

下面介绍如何在本地环境运行一个实时环路查找的图计算作业。

1. 启动流图作业

在编译完 geaflow 代码后，在工程目录下执行以下命令，启动实时环路查找的计算作业：

```shell
bin/gql_submit.sh --gql geaflow/geaflow-examples/gql/loop_detection.sql
```

其中 loop_detection.sql 是一段实时查询图中所有四度环路的 DSL 计算作业，其内容如下：

```sql
set geaflow.dsl.window.size = 1;
set geaflow.dsl.ignore.exception = true;

CREATE GRAPH IF NOT EXISTS dy_modern (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS tbl_source (
  text varchar
) WITH (
  type='socket',
  `geaflow.dsl.column.separator` = '#',
  `geaflow.dsl.socket.host` = 'localhost',
  `geaflow.dsl.socket.port` = 9003
);

CREATE TABLE IF NOT EXISTS tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint,
  a1_id bigint
) WITH (
  type='socket',
    `geaflow.dsl.column.separator` = ',',
    `geaflow.dsl.socket.host` = 'localhost',
    `geaflow.dsl.socket.port` = 9003
);

USE GRAPH dy_modern;

INSERT INTO dy_modern.person(id, name)
SELECT
cast(trim(split_ex(t1, ',', 0)) as bigint),
split_ex(t1, ',', 1)
FROM (
  Select trim(substr(text, 2)) as t1
  FROM tbl_source
  WHERE substr(text, 1, 1) = '.'
);

INSERT INTO dy_modern.knows
SELECT
 cast(split_ex(t1, ',', 0) as bigint),
 cast(split_ex(t1, ',', 1) as bigint),
 cast(split_ex(t1, ',', 2) as double)
FROM (
  Select trim(substr(text, 2)) as t1
  FROM tbl_source
  WHERE substr(text, 1, 1) = '-'
);

INSERT INTO tbl_result
SELECT DISTINCT
  a_id,
  b_id,
  c_id,
  d_id,
  a1_id
FROM (
  MATCH (a:person) -[:knows]->(b:person) -[:knows]-> (c:person)
   -[:knows]-> (d:person) -> (a:person)
  RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id, a.id as a1_id
);
```

该 DSL 实时读取 socket 服务 9003 端口数据，实时构图，然后计算图中所有的 4 度的环路, 并将环路上的点 id 输出到 socket 服务 9003 端口，然后显示在 socket 控制台。

2. 启动 SocketServer

执行以下命令，启动 socket server 程序:

```shell
bin/socket.sh
```

socket 服务启动后，控制台显示如下信息：

![socket_start](../static/img/socket_start.png)

3. 输入数据

输入数据如下，数据前面的"."代表一条点数据，"-"代表一条边数据(起点、终点和权重)。

```
. 1,jim
. 2,kate
. 3,lily
. 4,lucy
. 5,brown
. 6,jack
. 7,jackson
- 1,2,0.2
- 2,3,0.3
- 3,4,0.2
- 4,1,0.1
- 4,5,0.1
- 5,1,0.2
- 5,6,0.1
- 6,7,0.1
```

可以看到 socket 控制台上显示计算出来的环路数据：

![ide_socket_server](../static/img/ide_socket_server.png)

你也可以继续输入新的点边数据，查看最新计算结果，如输入一下数据：

```
- 6,3,0.1
```

可以看到新的环路 3-4-5-6-3 被检查出来：

![ide_socket_server_more](../static/img/ide_socket_server_more.png)

## GeaFlow Console 快速上手

GeaFlow Console 是 GeaFlow 提供的图计算研发平台，我们将介绍如何在 Docker 容器里面启动 GeaFlow Console 平台，提交流图计算作业。文档地址：
[文档](quick_start_docker.md)

## 使用 G6VP 进行流图计算作业可视化

G6VP 是一个可扩展的图可视分析平台，包括数据源管理、构图、图元素个性化配置、图可视分析等功能模块。使用 G6VP 能够很方便的对 Geaflow 计算结果进行可视化分析。文档地址：
[文档](visualization/collaborate_with_g6vp.md)
