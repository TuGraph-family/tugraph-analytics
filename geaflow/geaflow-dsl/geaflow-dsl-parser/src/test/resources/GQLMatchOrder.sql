MATCH (a:person where id = 1)-[e:knows where e.weight > 0.4]->(b:person)
Order by a, e, b RETURN a;

Match (a WHERE name = 'marko')<-[e]-(b) WHERE a.name <> b.name
Order by a.id, a.name, weight, b.id
RETURN e;

Match (a:person|animal|device WHERE name = 'where')-[e]-(b)
Order by b, e, a
RETURN b;

Match (a WHERE name = 'match')-[e]->(b)
Order by a ASC, b desc
RETURN a.name;

Match (a WHERE name = 'knows')<-[e]->(b)
Order by a.id desc, a.name desc, weight desc, b.id desc
RETURN e.test;

MATCH (a)->(b) - (c), (a) -> (d) <- (f)
Order by a, e, b limit 10
RETURN a, b;

MATCH (a)<-(b) <->(c), (c) -> (d) - (f)
limit 10
RETURN b, c, f;

