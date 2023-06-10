MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN b.name as name, b.id as b_id
THEN
FILTER b.amt > 100 AND a.age > 20;

MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
RETURN b.name as name, b.id as b_id
THEN
FILTER b.id > 10