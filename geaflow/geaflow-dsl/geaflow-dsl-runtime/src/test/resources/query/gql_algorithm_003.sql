CREATE TABLE tbl_result (
  vid int,
	prValue double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);


USE GRAPH dy_modern;

INSERT INTO dy_modern.person(id, name, age)
SELECT 1, 'jim', 20
UNION ALL
SELECT 2, 'kate', 22
;

INSERT INTO dy_modern.knows
SELECT 1, 2, 0.2
;

INSERT INTO dy_modern(person.id, person.name, knows.srcId, knows.targetId)
SELECT 3, 'jim', 3, 2
;

INSERT INTO tbl_result
CALL page_rank(1) YIELD (vid, prValue)
RETURN vid, ROUND(prValue, 2)
;