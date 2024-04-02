GeaFlow support the following aggregate functions:
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
Return count value for the aggregate group. The initial value is 0.

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
Return the maximum value for the aggregate group. The initial value is null.

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
Return the minimum value for the aggregate group. The initial value is null.

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
Return the sum of the aggregate group. The initial value is 0(or 0.0 for double).

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
Return the average value for the aggregate group. The initial value is null.

**Example**

```sql
select id, avg(age) from user group by id;
select avg(DISTINCT age) from user;
```