GeaFlow支持以下字符串函数:
* [ascii2str](#ascii2str)
* [base64_decode](#base64_decode)
* [base64_encode](#base64_encode)
* [concat](#concat)
* [concat_ws](#concat_ws)
* [hash](#hash)
* [index_of](#index_of)
* [instr](#instr)
* [isBlank](#isBlank)
* [length](#length)
* [like](#like)
* [lower](#lower)
* [ltrim](#ltrim)
* [regexp](#regexp)
* [regexp_count](#regexp_count)
* [regexp_extract](#regexp_extract)
* [repeat](#repeat)
* [replace](#replace)
* [reverse](#reverse)
* [rtrim](#rtrim)
* [space](#space)
* [split_ex](#split_ex)
* [substr](#substr)
* [trim](#trim)
* [upper](#upper)
* [urldecode](#urldecode)
* [urlencode](#urlencode)

# ascii2str
**Syntax**

```sql
string ascii2str(int ascii)
string ascii2str(long ascii)
```
**Description**
将数字转换为相应的ASCII字符。如果输入为空，则返回Null。

**Example**

```sql
ascii2str(66) = 'B'
ascii2str(48) = '0'
```

# base64_decode
**Syntax**

```sql
string base64_decode(string s)
```
**Description**
将Base64编码的字符串解码。如果输入为空，则返回Null。

**Example**

```sql
base64_decode('YWJjIA==') = 'abc '
base64_decode('dGVzdF9zdHJpbmc=') = 'test_string'
base64_decode(null) = null
```

# base64_encode
**Syntax**

```sql
string base64_encode(string s)
```
**Description**
将字符串编码为Base64格式。如果输入为空，则返回Null。

**Example**

```sql
base64_encode('abc ') = 'YWJjIA=='
base64_encode('test_string') = 'dGVzdF9zdHJpbmc='
```

# concat
**Syntax**

```sql
string concat(string... args)
```
**Description**
将按顺序传递的字符串连接成一个字符串。如果输入为空，则返回Null。

**Example**

```sql
concat('1',null,'2') = '12'
concat('1','2',null) = '12'
concat(null) = null;
```

# concat_ws
**Syntax**

```sql
string concat_ws(string separator, string... args)
```
**Description**
将所有的参数字符串用指定的分隔符连接起来。如果输入的分隔符为null，则使用空字符串作为分隔符。

**Example**

```sql
concat_ws(',','a','b','c')= 'a,b,c'
concat_ws(',','1','2','ant') = '1,2,ant'
concat_ws(',','1',null,'c') = '1,,c'
concat_ws(null, 'a','b','c') = 'abc'
```

# hash
**Syntax**

```sql
int hash(object s)
```
**Description**
返回输入对象的哈希码。如果输入为空，则返回Null。

**Example**

```sql
hash('1') = 49
hash(2) = 2
```

# index_of
**Syntax**

```sql
int index_of(string str, string target, int index)
int index_of(string str, string target)
```
**Description**
返回从索引位置开始，目标字符串在输入字符串中第一次出现的位置。如果未指定索引，则默认值为0。第一个字母的索引为0。如果任何输入为null，则返回-1。

**Example**

```sql
index_of('a test string', 'string', 3) = 7
index_of('a test string', 'test') = 2
index_of(null, 'test') = -1
```

# instr
**Syntax**

```sql
bigint instr(string str, string target)
bigint instr(string str, string target, bigint index, bigint nth)
```
**Description**
如果函数有两个输入参数，则返回目标字符串在字符串中第一次出现的位置（从1开始计数），从位置1开始查找。
如果函数有四个输入参数，则返回目标字符串在字符串中第nth次出现的位置（从1开始计数），从位置index开始查找。
如果任何输入为null，则返回Null。如果index < 1或nth < 1，则返回null。
如果目标字符串没有出现在字符串中，则返回0。

**Example**

```sql
instr('abc', 'a') = 1
instr('a test string', 'string', 3, 1) = 8
instr('abc', 'a', 3, -1) = null
instr('abc', null) = null
```

# isBlank
**Syntax**

```sql
boolean isBlank(string str)
```
**Description**
返回输入字符串是否为空白。如果输入为空，则返回true。

**Example**

```sql
isBlank('test') = false
isBlank(' ') = true
```

# length
**Syntax**

```sql
bigint length(string str)
```
**Description**
返回输入字符串的长度。如果输入为空，则返回Null。

**Example**

```sql
length('abc') = 3
length('abc  ') = 5
```

# like
**Syntax**

```sql
boolean like(string str, string likePattern)
```
**Description**
返回字符串是否与模式匹配。如果任何一个输入为空，则返回Null。

**Example**

```sql
like('abc', '%abc') = true
like('test', 'abc\\%') = false
like('abc', 'a%bc') = true
```

# lower
**Syntax**

```sql
string lower(string str)
```
**Description**
将字符串中的所有字符转换为小写字母并返回。如果输入为空，则返回Null。

**Example**

```sql
lower('ABC') = 'abc'
lower(null) = null
```

# ltrim
**Syntax**

```sql
string ltrim(string str)
```
**Description**
从输入字符串中删除前导空格字符并返回。如果输入为空，则返回Null。

**Example**

```sql
ltrim('    abc    ') = 'abc    '
ltrim('   test') = 'test'
```

# regexp
**Syntax**

```sql
boolean regexp(string str, string pattern)
```
**Description**
如果输入字符串与模式匹配，则返回true。如果任何一个输入为空，则返回Null。

**Example**

```sql
regexp('a.b.c.d.e.f', '.') = true
regexp('a.b.c.d.e.f', '.d%') = false
regexp('a.b.c.d.e.f', null) = null
```

# regexp_count
**Syntax**

```sql
bigint regexp(string str, string pattern)
bigint regexp(string str, string pattern, bigint startPos)
```
**Description**
返回与模式匹配的子字符串的数量。如果未指定startPos，则从位置0开始。如果任何输入为空，则返回Null。

**Example**

```sql
regexp('ab1d2d3dsss', '[0-9]d', 0) = 3
regexp('ab1d2d3dsss', '[0-9]d', 8) = 0
regexp('ab1d2d3dsss', '.b') = 1
```

# regexp_extract
**Syntax**

```sql
string regexp_extract(string str, string pattern)
string regexp_extract(string str, string pattern, bigint extractIndex)
```
**Description**
使用模式提取字符串并返回。如果未指定extractIndex，则从1开始。第一个字母的索引为1。如果任何输入为空，则返回Null。

**Example**

```sql
regexp_extract('abchebar', 'abc(.*?)(bar)', 1) = 'he'
regexp_extract('100-200', '(\d+)-(\d+)') = '100'
```

# regexp_replace
**Syntax**

```sql
string regexp_replace(string str, string pattern, string replacement)
```
**Description**
替换所有与模式匹配的子字符串。如果任何输入为空，则返回Null。

**Example**

```sql
regexp_replace('100-200', '(\\d+)', 'num') = 'num-num'
regexp_replace('(adfafa', '\\(', '') = 'adfafa'
regexp_replace('adfabadfasdf', '[a]', '3') = '3df3b3df3sdf'
```


# repeat
**Syntax**

```sql
string repeat(string str, int n)
```
**Description**
返回重复n次连接字符串str的结果。如果任何输入为空，则返回Null。

**Example**

```sql
repeat('abc', 3) = 'abcabcabc'
repeat(null, 4) = null
```

# replace
**Syntax**

```sql
string replace(string str, string oldString, string newString)
```
**Description**
将字符串str中的所有旧子字符串替换为新的子字符串。如果任何一个输入为空，则返回Null。

**Example**

```sql
replace('test test', 'test', 'c') = 'c c'
replace('test test', 'test', '') = ' '
```

# reverse
**Syntax**

```sql
string reverse(string str)
```
**Description**
返回反转后的字符串。如果输入为空，则返回Null。

**Example**

```sql
reverse('abc') = 'cba'
reverse(null) = null
```

# rtrim
**Syntax**

```sql
string rtrim(string str)
```
**Description**
从str中删除尾随空格字符。如果输入为空，则返回Null。

**Example**

```sql
rtrim('    abc    ') = '    abc'
rtrim('test') = 'test'
```

# space
**Syntax**

```sql
string space(bigint n)
```
**Description**
返回n个空格的字符串。如果输入为空，则返回Null。

**Example**

```sql
space(5) = '     '
space(null) = null
```

# split_ex
**Syntax**

```sql
string split_ex(string str, string separator, int nth)
```
**Description**
按分隔符separator拆分字符串str并返回第n个子字符串。如果任何一个输入为空或者nth < 0，则返回Null。

**Example**

```sql
split_ex('a.b.c.d.e', '.', 5) = null
split_ex('a.b.c.d.e', '.', 1) = 'b'
split_ex('a.b.c.d.e', '.', -1) = null
```


# substr
**Syntax**

```sql
string substr(string str, int pos)
string substr(string str, int pos, int len)
```
**Description**
从pos开始并具有长度为len的字符串的一部分。第一个字母的索引为1。如果未指定长度，则默认值为无穷大。如果任何输入为空，则返回Null。

**Example**

```sql
substr('testString', 5, 10) = 'String'
substr('testString', -6) = 'String'
```

# trim
**Syntax**

```sql
string trim(string str)
```
**Description**
删除字符串str中的前导和尾随空格字符。如果输入为空，则返回Null。

**Example**

```sql
trim('    abc    ') = 'abc'
trim('abc') = 'abc'
```

# upper
**Syntax**

```sql
string upper(string str)
```
**Description**
将字符串str中的所有字符转换为大写。如果输入为空，则返回Null。

**Example**

```sql
upper('abc') = 'ABC'
upper(null) = null
```

# urldecode
**Syntax**

```sql
string urldecode(string str)
```
**Description**
使用UTF-8解码URL。如果输入为空，则返回Null。

**Example**

```sql
urldecode('a%3d0%26c%3d1') = 'a=0&c=1'
urldecode('a%3D2') = 'a=2'
```

# urlencode
**Syntax**

```sql
string urlencode(string str)
```
**Description**
使用UTF-8编码URL。如果输入为空，则返回Null。

**Example**

```sql
urlencode('a=0&c=1') = 'a%3d0%26c%3d1'
urlencode('a=2') = 'a%3D2'
```