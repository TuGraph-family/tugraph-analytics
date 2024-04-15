Geaflow supports the following logical operations.

Operation|Description
----------------------|------
boolean1 OR boolean2  |	Return true if boolean1 is true or boolean2 is true.
boolean1 AND boolean2 |	Return true only if boolean1 is true and boolean2 is true.
NOT boolean	          | Return the result of a NOT operation for given boolean variable.
boolean IS FALSE      |	Return true if boolean variable is false. If boolean variable is UNKNOWN, return false.
boolean IS NOT FALSE  |	Return true if boolean variable is true. If boolean variable is UNKNOWN, return true.
boolean IS TRUE	      | Return true if boolean variable is true. If boolean variable is UNKNOWN, return false.
boolean IS NOT TRUE   |	Return true if boolean variable is false. If boolean variable is UNKNOWN, return true.
value1 =  value2      |	Return true if value1 is equal to value2.
value1 <> value2      |	Return true if value1 is not equal to value2.
value1 >  value2      |	Return true if value1 is greater than value2.
value1 >= value2      |	Return true if value1 is greater than or equal to value2.
value1 <  value2      |	Return true if value1 is smaller than value2.
value1 <= value2      |	Return true if value1 is smaller than or equal to value2.
value IS NULL         |	Return true if value is null.
value IS NOT NULL     |	Return true if value is not null.
value1 IS DISTINCT FROM value2       |	Return true if value1 is distinct from value2. If both value1 and value2 are null, they are considered equal.
value1 IS NOT DISTINCT FROM value2   |	Return true if value1 is equal to value2. If both value1 and value2 are null, they are considered equal.
value1 BETWEEN value2 AND value3     |  Return true if value1 is greater than or equal to value2 and smaller than value3.
value1 NOT BETWEEN value2 AND value3 |	Return true if value1 is smaller than value2 and greater than or equal to value3.
string1 LIKE string2 [ ESCAPE string3 ]	          | Perform fuzzy matching on the string string1, return true if it matches to pattern string2, and false if it doesn't match.
string1 NOT LIKE string2 [ ESCAPE string3 ]       |	Perform fuzzy matching on the string string1, return false if it matches to pattern string2, and true if it doesn't match.
value IN (value [, value]* )    |	Return true if value is equal to any value in the list.
value NOT IN (value [, value]* )|	Return true if value is not equal to every value in the list.