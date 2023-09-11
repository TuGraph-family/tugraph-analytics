CREATE TABLE tbl_result (
  a_id bigint,
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
  srcId,
  targetId
FROM (
  MATCH (a) <-[e:knows]-(b:person)
  RETURN a.id as a_id, e.srcId, e.targetId
)