CREATE TABLE console (
    currentid int,
    neighborid int,
	correlation double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO console
CALL jaccard(1) YIELD (currentid, neighborid, correlation)
RETURN cast (currentid as int), cast (neighborid as int), ROUND(correlation,2)
;