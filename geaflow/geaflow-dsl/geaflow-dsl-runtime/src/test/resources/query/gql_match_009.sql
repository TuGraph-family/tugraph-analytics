CREATE TABLE tbl_result (
  a_id bigint,
  weight bigint,
  b_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern_ts;

INSERT INTO tbl_result
SELECT
	a_id,
	ts,
	b_id
FROM (
Match
(a WHERE id in (1,2,3,4,5,6))-[e
WHERE (ts between 1 and 2 or ts between 2 and 3) and (ts between 2 and 3 or 3 < ts and 4 > ts)]-(b)
RETURN a.id as a_id, e.ts as ts, b.id as b_id
)
