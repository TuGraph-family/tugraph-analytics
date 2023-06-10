CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  p_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	p.id
FROM (
  MATCH
       (a:person where a.id = 4)-[e:created]->(b where b.id = 3)
       |+| (a:person where a.id = 4)-[e:created]->(b where b.id = 5)
       , (a:person where a.id = 4) <-[:knows]- (p)
  RETURN a, b, p
)
;

