/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

MATCH (n) RETURN n;

MATCH (n:Person) RETURN n;

MATCH (n:Person|Female) RETURN n;

MATCH (a) -[e]->(b) RETURN b;

MATCH (a: Label1 ) - (b:  Label2) RETURN a;

MATCH (a:Person WHERE a.age > 18) - (b: Person) RETURN b;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN a.name as name, b.id as b_id;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN a.name as name, b.id as b_id, b.amt * 10 as amt;

MATCH (a:person)-[e:knows where e.weight > 0.4]->(b:person)
RETURN a.id as a_id, SUM(ALL e.weight) * 10 as amt GROUP BY a_id;


MATCH (a:person)-[e:knows]->(b:person)
RETURN b.name as name, SUM(ALL b.age) as amt group by name;

MATCH (a:person)-[e:knows]->(b:person)
RETURN a.id as a_id, b.id as b_id, SUM(ALL e.weight) as e_sum GROUP BY a_id, b_id;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN b.name as name, b.id as _id, b.age as age order by age DESC;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN b.name as name, b.id as _id, b.age as age order by age DESC Limit 10;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN b.name as name, cast(b.id as int) as _id;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN b.name as name, case when b.type = 0 then '0' else '1' end as _id;

Match (a1 WHERE name like 'marko')-[e1]->(b1)
return b1.name AS b_id;

Match (a)-[e]->(b WHERE name = 'lop')
return a.id;
