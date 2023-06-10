MATCH (a:person where id = 1)-[e:knows where e.weight > 0.4]->(b:person) RETURN a;

Match (a WHERE name = 'marko')<-[e]-(b) WHERE a.name <> b.name RETURN e;

Match (a:person|animal|device WHERE name = 'where')-[e]-(b) RETURN b;

Match (a WHERE name = 'match')-[e]->(b) RETURN a.name;

Match (a WHERE name = 'knows')<-[e]->(b) RETURN e.test;

MATCH (a)->(b) - (c), (a) -> (d) <- (f) RETURN a, b;
MATCH (a)<-(b) <->(c), (c) -> (d) - (f) RETURN b, c, f;

