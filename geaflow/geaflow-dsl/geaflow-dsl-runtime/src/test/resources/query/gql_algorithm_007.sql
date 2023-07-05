CREATE TABLE console (
   vid int,
   cnt bigint
) WITH (
      type='file',
      geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO console
CALL triangle_count() YIELD (vid, cnt)
RETURN cast (vid as int), cnt
;