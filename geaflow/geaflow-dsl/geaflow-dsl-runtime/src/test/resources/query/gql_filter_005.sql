CREATE TABLE tbl_result (
  a_id bigint,
  weight double,
  b_id bigint
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
    a_id,
    weight,
    b_id
FROM (
Match (a where a.age is not null)-[e]-(b WHERE name in ('marko', 'josh', 'vadas', 'peter'))
RETURN a.id as a_id, e.weight as weight, b.id as b_id
)
