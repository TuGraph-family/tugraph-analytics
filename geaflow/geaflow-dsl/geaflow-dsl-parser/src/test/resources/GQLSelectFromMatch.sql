SELECT name, b_id FROM (
  SELECT name, b_id FROM test_table
)
;

SELECT name, b_id FROM (
  MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
	RETURN a.name as name, b.id as b_id
)
;

-- from match-return
SELECT name, b_id FROM (
    MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
  	RETURN a, e, b
)
;

SELECT name, b_id FROM (
  MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person)
	RETURN a.name as name, b.id as b_id
)
UNION ALL
SELECT name, b_id FROM (
  MATCH (a:person WHERE a.id = '2')-[e:knows]->(b:person)
	RETURN a.name as name, b.id as b_id
)
;

SELECT id, cnt
FROM (
  MATCH (a:person WHERE a.id = p.id)-[e:knows]->(b:person where b.name = p.name)
  RETURN a.id as id, count(b.id) as cnt GROUP BY a.id
)
;

SELECT id
FROM (
  MATCH (a:person WHERE a.id = p.id)-[e:knows]->(b:person where b.name = p.name)
  RETURN n.id as id
)
;

CREATE VIEW view_1 (id) AS
SELECT id
FROM (
  MATCH (a:person WHERE a.id = p.id)-[e:knows]->(b:person where b.name = p.name)
  RETURN n.id as id
)
;
