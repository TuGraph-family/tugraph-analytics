GeaFlow支持以下聚合函数：
* [COUNT](#COUNT)
* [MAX](#MAX)
* [MIN](#MIN)
* [SUM](#SUM)
* [AVG](#AVG)

# COUNT
**Syntax**

```sql
long count([DISTINCT] Expr)
```
**Description**
COUNT聚合函数返回聚合分组中的计数数值。聚合分组的初始值为0。

**Example**

```sql
select id, count(id) from user group by id;
select count(distinct id) from user;
select count(1) from user;
```

# MAX
**Syntax**

```sql
int max(int value)
long max(long value)
double max(double value)
varchar max(varchar value)
```
**Description**
MAX聚合函数返回聚合分组中的最大值。聚合分组的初始值为null。

**Example**

```sql
select id, max(age) from user group by id;
select max(name) from user;
```

# MIN
**Syntax**

```sql
int min(int value)
long min(long value)
double min(double value)
varchar min(varchar value)
```
**Description**
MIN聚合函数返回聚合分组中的最小值。聚合分组的初始值为null。

**Example**

```sql
select id, min(age) from user group by id;
select min(name) from user;
```

# SUM
**Syntax**

```sql
int sum([DISTINCT] int value)
long sum([DISTINCT] long value)
double sum([DISTINCT] double value)
```
**Description**
SUM聚合函数返回聚合分组中的数值总和。对于整数类型，聚合分组的初始值为0；对于浮点数类型，聚合分组的初始值为0.0。

**Example**

```sql
select id, sum(age) from user group by id;
select sum(DISTINCT age) from user;
select sum(1) from user;
```

# AVG
**Syntax**

```sql
int avg([DISTINCT] int value)
long avg([DISTINCT] long value)
double avg([DISTINCT] double value)
```
**Description**
AVG聚合函数返回聚合分组中的数值平均值。聚合分组的初始值为null。

**Example**

```sql
select id, avg(age) from user group by id;
select avg(DISTINCT age) from user;
```