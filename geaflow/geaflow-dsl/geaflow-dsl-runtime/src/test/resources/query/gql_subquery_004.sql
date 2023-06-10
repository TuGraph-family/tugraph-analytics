CREATE TABLE tbl_result (
  a_id bigint,
  weight double,
  b_id bigint,
  b_name varchar,
  b_out_cnt bigint,
  b_out_weight double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	e.weight,
	b.id,
	b.name,
	b.out_cnt,
	b.out_weight
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  Where COUNT((b) ->(c) => c) >= 1 And SUM((b) -[e1]-> (c) => e1.weight) > 1
  Let b.out_cnt = COUNT((b) ->(c) => c),
  Let b.out_weight = SUM((b) -[e1]-> (c) => e1.weight)
  RETURN a, e, b
)