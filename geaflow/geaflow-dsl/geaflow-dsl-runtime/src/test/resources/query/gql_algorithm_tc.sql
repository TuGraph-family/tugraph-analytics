CREATE TABLE result_tb (
   vid int,
   cnt bigint
) WITH (
      type='file',
      geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO result_tb
CALL triangle_count("person") YIELD (vid, cnt)
RETURN cast (vid as int), cnt
;