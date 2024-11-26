GeaFlow support the following string functions.
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
Converts numbers to corresponding ascii characters. Return Null if input is null.

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
Decodes the string from Base64. Return Null if input is null.

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
Encodes the string to Base64. Return Null if input is null.

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
Returns the string of concatenating the strings passed in as parameters in order. Return Null if input is null.

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
Concatenates all strings in arguments, separated by the separator. Use an empty string as separator if the input separator is null.

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
Returns the hash code of input object. Return Null if input is null.

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
Returns the position of the target string in the input string from position index. If index is not specified, default value is 0. The index of the first letter is 0. Return -1 if any of the input is null.

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
If there are 2 input args in function, return the location where target string first appeared in the string (counting from 1) from position 1.
If there are 4 input args in function, return the location where target string nth time appeared in the string (counting from 1) from position index.
Return Null if any of the input is null. If index < 1 or nth < 1, return null.
If target string does not appear in str, return 0.

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
Returns whether the input string is blank. Return true if input is null.

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
Returns the length of the input string. Return Null if input is null.

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
Returns whether string matches to the pattern. Return Null if any of the input is null.

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
Returns str with all characters converted to lowercase. Return Null if input is null.

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
Removes the leading space characters from input string. Return Null if input is null.

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
Returns true if input string matches to pattern. Return Null if any of the input is null.

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
Returns the number of substring which matches to pattern. If startPos is not specified, start from position 0. Return Null if any of the input is null.

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
Returns the string extracted using the pattern. If extractIndex is not specified, start from 1. The index of the first letter is 1. Return Null if any of the input is null.

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
Replaces all substrings of str that matching the pattern. Return Null if any of the input is null.

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
Returns the result of repeating concatenation of string str n times. Return Null if any of the input is null.

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
Replaces all old substring with new substring in str. Return Null if any of the input is null.

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
Returns the reversed string. Return Null if input is null.

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
Removes the trailing space characters from str. Return Null if input is null.

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
Returns a string of n spaces. Return Null if input is null.

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
Splits str by separator and returns the nth substring. Return Null if any of the input is null or nth < 0.

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
Returns the part of the string described by the first parameter starting from pos and having a length of len. The index of the first letter is 1. If length is not specified, default value is infinity. Return Null if any of the input is null.

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
Removes the leading and trailing space characters from str. Return Null if input is null.

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
Returns str with all characters converted to uppercase. Return Null if input is null.

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
Decodes the URL using UTF-8. Return Null if input is null.

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
Eecodes the URL using UTF-8. Return Null if input is null.

**Example**

```sql
urlencode('a=0&c=1') = 'a%3d0%26c%3d1'
urlencode('a=2') = 'a%3D2'
```