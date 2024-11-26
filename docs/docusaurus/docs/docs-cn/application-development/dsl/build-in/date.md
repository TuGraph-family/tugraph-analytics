GeaFlow支持以下日期函数：
* [from_unixtime](#from_unixtime)
* [from_unixtime_millis](#from_unixtime_millis)
* [unix_timestamp](#unix_timestamp)
* [unix_timestamp_millis](#unix_timestamp_millis)
* [isdate](#isdate)
* [now](#now)
* [day](#day)
* [weekday](#weekday)
* [lastday](#lastday)
* [day_of_month](#day_of_month)
* [week_of_year](#week_of_year)
* [date_add](#date_add)
* [date_sub](#date_sub)
* [date_diff](#date_diff)
* [add_months](#add_months)
* [date_format](#date_format)
* [date_part](#date_part)
* [date_trunc](#date_trunc)

# from_unixtime
**Syntax**

```sql
string from_unixtime(int unixtime)
string from_unixtime(long unixtime)
string from_unixtime(int unixtime, string format)
string from_unixtime(long unixtime, string format)
string from_unixtime(string unixtime, string format)
```
**Description**
将Unix时间戳转换成时间格式， 默认格式为“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回Null。

**Example**

```sql
from_unixtime(11111111) = '1970-05-09 22:25:11'
from_unixtime(11111111, 'yyyy-MM-dd HH:mm:ss.SSSSSS') = '1970-05-09 22:25:11.000000'
```

# from_unixtime_millis
**Syntax**

```sql
string from_unixtime_millis(int unixtime)
string from_unixtime_millis(long unixtime)
string from_unixtime_millis(string unixtime)
string from_unixtime_millis(int unixtime, string format)
string from_unixtime_millis(long unixtime, string format)
```
**Description**
将Unix毫秒时间戳转换成时间格式， 默认格式为“yyyy-MM-dd HH:mm:ss.SSS”。如果任何输入为空，则返回Null。
**Example**

```sql
from_unixtime_millis(11111111) = '1970-01-01 11:05:11.111'
from_unixtime_millis(11111111, 'yyyy-MM-dd HH:mm:ss') = '1970-01-01 11:05:11'
from_unixtime_millis(11111111, 'yyyy-MM-dd HH:mm:ss.SSSSSS') = '1970-01-01 11:05:11.111000'
```

# unix_timestamp
**Syntax**

```sql
long unix_timestamp()
long unix_timestamp(string dateText)
long unix_timestamp(string dateText, string patternText)
```
**Description**
返回Unix时间戳。如果未提供任何参数，则返回当前时间戳。如果给出dateText，则返回相应的时间戳。当未指定格式文本patternText时，则使用默认格式“yyyy-MM-dd”、“yyyy-MM-dd HH:mm:ss”或“yyyy-MM-dd HH:mm:ss.SSSSSS”解析dateText。如果任何输入为空，则返回Null。

**Example**

```sql
unix_timestamp('1987-06-05 00:11:22') = 549817882
unix_timestamp('1987-06-05 00:11', 'yyyy-MM-dd HH:mm') = 549817860
```

# unix_timestamp_millis
**Syntax**

```sql
long unix_timestamp_millis()
long unix_timestamp_millis(string dateText)
long unix_timestamp_millis(string dateText, string patternText)
```
**Description**
返回Unix毫秒时间戳。类似于函数**unix_timestamp**。
**Example**

```sql
unix_timestamp_millis('1987-06-05 00:11:22') = 549817882000
unix_timestamp_millis('1987-06-05', 'yyyy-mm-dd') = 536774760000
```

# isdate
**Syntax**

```sql
boolean isdate(string date)
boolean isdate(string date, string format)
```
**Description**
判断字符串是否为指定格式的日期。默认格式为“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回false。

**Example**

```sql
isdate('1987-06-05 00:11:22') = true
isdate('xxxxxxxxxxxxx') = false
isdate('1987-06-05 00:11:22', 'yyyy-MM-dd HH:mm:ss.SSSSSS') = false
```

# now
**Syntax**

```sql
long now()
long now(int offset)
long now(long offset)
```
**Description**
返回带有可选偏移量的当前时间戳。

**Example**

```sql
now()
now(1000)
```

# day
**Syntax**

```sql
int day(string dateString)
```
**Description**
返回指定日期的天数。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回null。

**Example**

```sql
day('1987-06-05 00:11:22') = 5
```

# weekday
**Syntax**

```sql
int weekday(string dateString)
```
**Description**
返回指定日期的星期几。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回null。

**Example**

```sql
weekday('1987-06-05 00:11:22') = 5
```

# lastday
**Syntax**

```sql
string lastday(string dateString)
```
**Description**
返回指定日期所在月份的最后一天。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回null。

**Example**

```sql
lastday('1987-06-05') = '1987-06-30 00:00:00'
```

# day_of_month
**Syntax**

```sql
int day_of_month(string dateString)
```
**Description**
返回指定日期所在月份的日期。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回null。

**Example**

```sql
day_of_month('1987-06-05 00:11:22') = 5
```

# week_of_year
**Syntax**

```sql
int week_of_year(string dateString)
```
**Description**
返回给定日期所在年份的周数。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回null。

**Example**

```sql
week_of_year('1987-06-05 00:11:22') = 23
```


# date_add
**Syntax**

```sql
string date_add(string date, int days)
```
**Description**
将指定日期增加指定天数。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回Null。

**Example**

```sql
date_add('2017-09-25 10:00:00', 1) = '2017-09-26'
date_add('2017-09-25', 1) = '2017-09-26'
date_add('2017-09-25', -1) = '2017-09-24'
```

# date_sub
**Syntax**

```sql
string date_sub(string date, int days)
```
**Description**
将指定日期减去指定天数。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回Null。

**Example**

```sql
date_sub('2017-09-25 10:00:00', 1) = '2017-09-24'
date_sub('2017-09-25', 1) = '2017-09-24'
date_sub('2017-09-25', -1) = '2017-09-26'
```

# date_diff
**Syntax**

```sql
int date_diff(string dateString1, string dateString2)
```
**Description**
返回从dateString2到dateString1的天数。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回Null。

**Example**

```sql
date_diff('2017-09-26', '2017-09-25') = 1
date_diff('2017-09-24', '2017-09-25') = -1
```

# add_months
**Syntax**

```sql
string add_months(string date, int months)
```
**Description**
将指定日期增加指定月份。默认格式为“yyyy-MM-dd”或“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回Null。

**Example**

```sql
add_months('2017-09-25 10:00:00', 1) = '2017-10-25 10:00:00'
add_months('2017-09-25', 1) = '2017-10-25'
add_months('2017-09-25', -1) = '2017-08-25'
```

# date_format
**Syntax**

```sql
string date_format(string dateText)
string date_format(string dateText, string toFormat)
string date_format(string dateText, string fromFormat, string toFormat)
```
**Description**
将日期从一种格式转换为另一种格式。默认的fromFormat为“yyyy-MM-dd HH:mm:ss”或“yyyy-MM-dd HH:mm:ss.SSSSSS”，默认的toFormat为“yyyy-MM-dd HH:mm:ss”。如果任何输入为空，则返回Null。

**Example**

```sql
date_format('1987-06-05 00:11:22') = '1987-06-05 00:11:22'
date_format('1987-06-05 00:11:22', 'MM-dd-yyyy') = '06-05-1987'
date_format('00:11:22 1987-06-05', 'HH:mm:ss yyyy-MM-dd', 'MM-dd-yyyy') = '06-05-1987'
```

# date_part
**Syntax**

```sql
int date_part(string dateText, string datePart)
```
**Description**
按日期部分格式返回日期的部分。默认日期格式为“yyyy-MM-dd HH:mm:ss”或“yyyy-MM-dd”。如果任何输入为空，则返回Null。datePart格式如下表所示。

| Select | datePart |
| -------- | -------- |
| 年     | yyyy, year     |
| 月     | mm, mon, month     |
| 月中天数     | dd, day     |
| 一天中小时数     | hh, hour     |
| 分     | mi, minute     |
| 秒     | ss, second     |

**Example**

```sql
date_part('1987-06-05 00:11:22', 'yyyy') = 1987
date_part('1987-06-05 00:11:22', 'mm') = 6
date_part('1987-06-05 00:11:22', 'dd') = 5
date_part('1987-06-05 00:11:22', 'hh') = 0
date_part('1987-06-05 00:11:22', 'mi') = 11
date_part('1987-06-05 00:11:22', 'ss') = 22
date_part('1987-06-05', 'ss') = 0
```

# date_trunc
**Syntax**

```sql
string date_trunc(string dateText, string datePart)
```
**Description**
按指定格式将日期截断。默认日期格式为“yyyy-MM-dd HH:mm:ss”或“yyyy-MM-dd”。如果任何输入为空，则返回Null。datePart格式与函数**date_part**相同。

**Example**

```sql
date_trunc('1987-06-05 00:11:22', 'yyyy') = '1987-01-01 00:00:00'
date_trunc('1987-06-05 00:11:22', 'mm') = '1987-06-01 00:00:00'
date_trunc('1987-06-05 00:11:22', 'dd') = '1987-06-05 00:00:00'
date_trunc('1987-06-05 00:11:22', 'hh') = '1987-06-05 00:00:00'
date_trunc('1987-06-05 00:11:22', 'mi') = '1987-06-05 00:11:00'
date_trunc('1987-06-05 00:11:22', 'ss') = '1987-06-05 00:11:22'
date_trunc('1987-06-05', 'ss') = '1987-01-01 00:00:00'
```