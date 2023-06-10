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
