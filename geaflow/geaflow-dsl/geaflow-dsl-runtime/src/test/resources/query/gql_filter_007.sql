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
    a.id,
    b.id
FROM (
Match (a)-[e]-(b)
RETURN a, e, b
THEN FILTER e.weight > 0.5 and a.age is not null
)