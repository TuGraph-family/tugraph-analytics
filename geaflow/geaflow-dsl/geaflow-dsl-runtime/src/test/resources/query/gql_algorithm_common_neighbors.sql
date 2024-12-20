CREATE TABLE result_tb (
   vid int
) WITH (
      type='file',
      geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO result_tb
CALL common_neighbors(1, 3) YIELD (id)
RETURN cast (id as int)
;