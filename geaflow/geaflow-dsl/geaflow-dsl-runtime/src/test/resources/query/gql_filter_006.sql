CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
    a_id,
    b_id
FROM (
Match (a where a.age is not null)-[e]-(b) where e.weight > 0.5
RETURN a.id as a_id, b.id as b_id
)